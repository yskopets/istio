// Copyright Istio Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package workloadentry

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"istio.io/istio/pilot/pkg/features"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/workloadentry/internal/autoregistration"
	"istio.io/istio/pilot/pkg/workloadentry/internal/externalregistration"
	"istio.io/istio/pilot/pkg/workloadentry/internal/health"
	workloadentrystore "istio.io/istio/pilot/pkg/workloadentry/internal/store"
	"istio.io/istio/pkg/config"
	"istio.io/istio/pkg/config/schema/gvk"
	"istio.io/istio/pkg/kube/controllers"
	"istio.io/istio/pkg/queue"
	istiolog "istio.io/pkg/log"
)

const (
	// maxRetries is the number of times a service will be retried before it is dropped out of the queue.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the
	// sequence of delays between successive queuings of a service.
	//
	// 5ms, 10ms, 20ms, 40ms, 80ms
	maxRetries = 5
)

var log = istiolog.RegisterScope("wle", "wle controller debugging", 0)

type HealthEvent = health.HealthEvent

var WorkloadControllerAnnotation = workloadentrystore.WorkloadControllerAnnotation

type WorkloadEntryCleaner interface {
	GetCleanupGracePeriod() time.Duration
	ShouldCleanup(wle *config.Config, maxConnectionAge time.Duration) bool
	Cleanup(wle *config.Config)
}

type Controller struct {
	instanceID string
	// TODO move WorkloadEntry related tasks into their own object and give InternalGen a reference.
	// store should either be k8s (for running pilot) or in-memory (for tests). MCP and other config store implementations
	// do not support writing. We only use it here for reading WorkloadEntry/WorkloadGroup.
	store model.ConfigStoreController

	// Note: unregister is to update the workload entry status: like setting `DisconnectedAtAnnotation`
	// and make the workload entry enqueue `cleanupQueue`
	// cleanup is to delete the workload entry

	// queue contains workloadEntry that need to be unregistered
	queue controllers.Queue
	// cleanupLimit rate limit's auto registered WorkloadEntry cleanup calls to k8s
	cleanupLimit *rate.Limiter
	// cleanupQueue delays the cleanup of auto registered WorkloadEntries to allow for grace period
	cleanupQueue queue.Delayed

	mutex sync.Mutex
	// record the current adsConnections number
	// note: this is to handle reconnect to the same istiod, but in rare case the disconnect event is later than the connect event
	// keyed by proxy network+ip
	adsConnections map[string]uint8

	// maxConnectionAge is a duration that workload entry should be cleaned up if it does not reconnects.
	maxConnectionAge time.Duration

	wleStore             *workloadentrystore.Controller
	healthController     *health.Controller
	autoregistration     *autoregistration.Controller
	externalregistration *externalregistration.Controller
}

// NewController create a controller which manages workload lifecycle and health status.
func NewController(store model.ConfigStoreController, instanceID string, maxConnAge time.Duration) *Controller {
	if features.WorkloadEntryAutoRegistration || features.WorkloadEntryHealthChecks {
		maxConnAge := maxConnAge + maxConnAge/2
		// if overflow, set it to max int64
		if maxConnAge < 0 {
			maxConnAge = time.Duration(math.MaxInt64)
		}
		c := &Controller{
			instanceID:       instanceID,
			store:            store,
			cleanupLimit:     rate.NewLimiter(rate.Limit(20), 1),
			cleanupQueue:     queue.NewDelayed(),
			adsConnections:   map[string]uint8{},
			maxConnectionAge: maxConnAge,
		}
		c.queue = controllers.NewQueue("unregister_workloadentry",
			controllers.WithMaxAttempts(maxRetries),
			controllers.WithGenericReconciler(c.unregisterWorkload))
		c.wleStore = workloadentrystore.NewController(store, instanceID)
		c.healthController = health.NewController(c.wleStore, maxRetries)
		c.autoregistration = autoregistration.NewController(store, c.wleStore)
		c.externalregistration = externalregistration.NewController(c.wleStore)
		return c
	}
	return nil
}

func (c *Controller) Run(stop <-chan struct{}) {
	if c == nil {
		return
	}
	if c.store != nil && c.cleanupQueue != nil {
		go c.periodicWorkloadEntryCleanup(stop)
		go c.cleanupQueue.Run(stop)
	}

	go c.queue.Run(stop)
	go c.healthController.Run(stop)
	<-stop
}

// workItem contains the state of a "disconnect" event used to unregister a workload.
type workItem struct {
	entryName   string
	autoCreated bool
	proxy       *model.Proxy
	disConTime  time.Time
	origConTime time.Time
}

// RegisterWorkload determines whether a connecting proxy represents a non-Kubernetes
// workload and, if that's the case, initiates special processing required for that type
// of workloads, such as auto-registration, health status updates, etc.
//
// If connecting proxy represents a workload that is using auto-registration, it will
// create a WorkloadEntry resource automatically and be ready to receive health status
// updates.
//
// If connecting proxy represents a workload that is not using auto-registration,
// the WorkloadEntry resource is expected to exist beforehand. Otherwise, no special
// processing will be initiated, e.g. health status updates will be ignored.
func (c *Controller) RegisterWorkload(proxy *model.Proxy, conTime time.Time) error {
	if c == nil {
		return nil
	}
	var entryName string
	var autoCreate bool
	if autoregistration.IsApplicableTo(proxy) {
		entryName = autoregistration.GenerateWorkloadEntryName(proxy)
		autoCreate = true
	} else if externalregistration.IsApplicableTo(proxy) {
		// a non-empty value of the `WorkloadEntry` field indicates that proxy must correspond to the WorkloadEntry
		wle := c.wleStore.Get(proxy.Metadata.WorkloadEntry, proxy.Metadata.Namespace)
		if wle == nil {
			// either invalid proxy configuration or config propagation delay
			return fmt.Errorf("proxy metadata indicates that it must correspond to an existing WorkloadEntry, "+
				"however WorkloadEntry %s/%s is not found", proxy.Metadata.Namespace, proxy.Metadata.WorkloadEntry)
		}
		if health.IsElegibleForHealthStatusUpdates(wle) {
			entryName = wle.Name
		}
	}
	if entryName == "" {
		return nil
	}
	proxy.WorkloadEntryName = entryName
	proxy.WorkloadEntryAutoCreated = autoCreate

	c.mutex.Lock()
	c.adsConnections[makeProxyKey(proxy)]++
	c.mutex.Unlock()

	err := c.onWorkloadConnect(proxy, conTime)
	if err != nil {
		log.Error(err)
	}
	return err
}

// onWorkloadConnect creates/updates WorkloadEntry of the connecting workload.
//
// If workload is using auto-registration, WorkloadEntry will be created automatically.
//
// If workload is not using auto-registration, WorkloadEntry must already exist.
func (c *Controller) onWorkloadConnect(proxy *model.Proxy, conTime time.Time) error {
	if proxy.WorkloadEntryAutoCreated {
		return c.autoregistration.OnWorkloadConnect(proxy, conTime)
	}
	return c.externalregistration.OnWorkloadConnect(proxy, conTime)
}

// QueueUnregisterWorkload determines whether a connected proxy represents a non-Kubernetes
// workload and, if that's the case, terminates special processing required for that type
// of workloads, such as auto-registration, health status updates, etc.
//
// If proxy represents a workload (be it auto-registered or not), WorkloadEntry resource
// will be updated to reflect that the proxy is no longer connected to this particular `istiod`
// instance.
//
// Besides that, if proxy represents a workload that is using auto-registration, WorkloadEntry
// resource will be scheduled for removal if the proxy does not reconnect within a grace period.
//
// If proxy represents a workload that is not using auto-registration, WorkloadEntry resource
// will be scheduled to be marked unhealthy if the proxy does not reconnect within a grace period.
func (c *Controller) QueueUnregisterWorkload(proxy *model.Proxy, origConnect time.Time) {
	if c == nil {
		return
	}
	if !features.WorkloadEntryAutoRegistration && !features.WorkloadEntryHealthChecks {
		return
	}
	// check if the WE already exists, update the status
	entryName := proxy.WorkloadEntryName
	if entryName == "" {
		return
	}

	c.mutex.Lock()
	proxyKey := makeProxyKey(proxy)
	num := c.adsConnections[proxyKey]
	// if there is still ads connection, do not unregister.
	if num > 1 {
		c.adsConnections[proxyKey] = num - 1
		c.mutex.Unlock()
		return
	}
	delete(c.adsConnections, proxyKey)
	c.mutex.Unlock()

	workload := &workItem{
		entryName:   entryName,
		autoCreated: proxy.WorkloadEntryAutoCreated,
		proxy:       proxy,
		disConTime:  time.Now(),
		origConTime: origConnect,
	}
	// queue has max retry itself
	c.queue.Add(workload)
}

func (c *Controller) unregisterWorkload(item any) error {
	workItem, ok := item.(*workItem)
	if !ok {
		return nil
	}

	changed, err := c.wleStore.ChangeStateToDisconnected(workItem.entryName, workItem.proxy.Metadata.Namespace, workItem.disConTime, workItem.origConTime)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}

	var cleaner WorkloadEntryCleaner
	if workItem.autoCreated {
		cleaner = c.autoregistration.OnWorkloadDisconnect()
	} else {
		cleaner = c.externalregistration.OnWorkloadDisconnect()
	}

	// after grace period, check if the workload ever reconnected
	ns := workItem.proxy.Metadata.Namespace
	c.cleanupQueue.PushDelayed(c.newCleanTask(workItem.entryName, ns, cleaner), cleaner.GetCleanupGracePeriod())

	return nil
}

func (c *Controller) newCleanTask(entryName, entryNs string, cleaner WorkloadEntryCleaner) queue.Task {
	return func() error {
		wle := c.wleStore.Get(entryName, entryNs)
		if wle == nil {
			return nil
		}
		if !cleaner.ShouldCleanup(wle, c.maxConnectionAge) {
			return nil
		}
		if err := c.cleanupLimit.Wait(context.TODO()); err != nil {
			log.Errorf("error in WorkloadEntry cleanup rate limiter: %v", err)
			return nil
		}
		cleaner.Cleanup(wle)
		return nil
	}
}

// QueueWorkloadEntryHealth enqueues the associated WorkloadEntries health status.
func (c *Controller) QueueWorkloadEntryHealth(proxy *model.Proxy, event health.HealthEvent) {
	c.healthController.QueueWorkloadEntryHealth(proxy, event)
}

// periodicWorkloadEntryCleanup checks lists all WorkloadEntry
func (c *Controller) periodicWorkloadEntryCleanup(stopCh <-chan struct{}) {
	if !features.WorkloadEntryAutoRegistration && !features.WorkloadEntryHealthChecks {
		return
	}
	ticker := time.NewTicker(10 * features.WorkloadEntryCleanupGracePeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			wles := c.store.List(gvk.WorkloadEntry, metav1.NamespaceAll)
			for _, wle := range wles {
				wle := wle
				if autoregistration.IsAutoRegisteredWorkloadEntry(&wle) {
					if c.autoregistration.ShouldCleanup(&wle, c.maxConnectionAge) {
						c.cleanupQueue.Push(c.newCleanTask(wle.Name, wle.Namespace, c.autoregistration))
					}
				}
				if externalregistration.IsHealthCheckedWorkloadEntry(&wle) {
					if c.externalregistration.ShouldCleanup(&wle, c.maxConnectionAge) {
						c.cleanupQueue.Push(c.newCleanTask(wle.Name, wle.Namespace, c.externalregistration))
					}
				}
			}
		case <-stopCh:
			return
		}
	}
}

func makeProxyKey(proxy *model.Proxy) string {
	key := strings.Join([]string{
		string(proxy.Metadata.Network),
		proxy.IPAddresses[0],
		proxy.WorkloadEntryName,
		proxy.Metadata.Namespace,
	}, "~")
	return key
}

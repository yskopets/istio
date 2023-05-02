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

package externalregistration

import (
	"time"

	"istio.io/istio/pilot/pkg/features"
	"istio.io/istio/pilot/pkg/model"
	"istio.io/istio/pilot/pkg/workloadentry/internal/autoregistration"
	"istio.io/istio/pilot/pkg/workloadentry/internal/health"
	workloadentrystore "istio.io/istio/pilot/pkg/workloadentry/internal/store"
	"istio.io/istio/pkg/config"
	istiolog "istio.io/pkg/log"
)

var log = istiolog.RegisterScope("wle", "wle controller debugging", 0)

// Controller manages lifecycle of those workloads that are not using auto-registration.
type Controller struct {
	wleStore *workloadentrystore.Controller
}

// NewController returns a new Controller instance.
func NewController(wleStore *workloadentrystore.Controller) *Controller {
	return &Controller{
		wleStore: wleStore,
	}
}

func IsApplicableTo(proxy *model.Proxy) bool {
	return features.WorkloadEntryHealthChecks && proxy.Metadata.WorkloadEntry != ""
}

// OnWorkloadConnect updates an existing WorkloadEntry of a workload that is not using
// auto-registration.
func (c *Controller) OnWorkloadConnect(proxy *model.Proxy, conTime time.Time) error {
	changed, err := c.wleStore.ChangeStateToConnected(proxy.WorkloadEntryName, proxy.Metadata.Namespace, conTime)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}
	log.Infof("updated health-checked WorkloadEntry %s/%s", proxy.Metadata.Namespace, proxy.WorkloadEntryName)
	return nil
}

// OnWorkloadDisconnect handles workload disconnect.
func (c *Controller) OnWorkloadDisconnect() *Controller {
	return c
}

// GetCleanupGracePeriod implements WorkloadEntryCleaner.
func (c *Controller) GetCleanupGracePeriod() time.Duration {
	return features.WorkloadEntryCleanupGracePeriod
}

// ShouldCleanup implements WorkloadEntryCleaner.
func (c *Controller) ShouldCleanup(wle *config.Config, maxConnectionAge time.Duration) bool {
	return IsHealthCheckedWorkloadEntry(wle) && health.HasHealthCondition(wle) &&
		workloadentrystore.IsExpired(wle, maxConnectionAge, c.GetCleanupGracePeriod())
}

// Cleanup updates WorkloadEntry of a workload that is not using auto-registration
// to remove information about the health status (since we can no longer be certain about it).
func (c *Controller) Cleanup(wle *config.Config) {
	if !IsHealthCheckedWorkloadEntry(wle) {
		return
	}
	err := c.wleStore.DeleteHealthCondition(*wle)
	if err != nil {
		log.Warnf("failed cleaning up health-checked WorkloadEntry %s/%s: %v", wle.Namespace, wle.Name, err)
		return
	}
	log.Infof("cleaned up health-checked WorkloadEntry %s/%s", wle.Namespace, wle.Name)
}

func IsHealthCheckedWorkloadEntry(wle *config.Config) bool {
	return wle != nil && workloadentrystore.IsControlled(wle) && !autoregistration.IsAutoRegisteredWorkloadEntry(wle)
}

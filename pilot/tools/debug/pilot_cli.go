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

// Tool to get xDS configs from pilot. This tool simulate envoy sidecar gRPC call to get config,
// so it will work even when sidecar haswhen sidecar hasn't connected (e.g in the case of pilot running on local machine))
//
// Usage:
//
// First, you can either manually expose pilot gRPC port or rely on this tool to port-forward pilot by omitting -pilot_url flag:
//
// * By port-forward existing pilot:
// ```bash
// kubectl port-forward $(kubectl get pod -l app=istiod -o jsonpath='{.items[0].metadata.name}' -n istio-system) -n istio-system 15010
// ```
// * Or run local pilot using the same k8s config.
// ```bash
// pilot-discovery discovery --kubeconfig=${HOME}/.kube/config
// ```
//
// To get LDS or CDS, use -type lds or -type cds, and provide the pod id or app label. For example:
// ```bash
// go run pilot_cli.go --type lds --proxytag httpbin-5766dd474b-2hlnx  # --res will be ignored
// go run pilot_cli.go --type lds --proxytag httpbin
// ```
// Note If more than one pod match with the app label, one will be picked arbitrarily.
//
// For EDS/RDS, provide comma-separated-list of corresponding clusters or routes name. For example:
// ```bash
// go run ./pilot/tools/debug/pilot_cli.go --type eds --proxytag httpbin \
// --res "inbound|http||sleep.default.svc.cluster.local,outbound|http||httpbin.default.svc.cluster.local"
// ```
//
// Script requires kube config in order to connect to k8s registry to get pod information (for LDS and CDS type). The default
// value for kubeconfig path is .kube/config in home folder (works for Linux only). It can be changed via -kubeconfig flag.
// ```bash
// go run ./pilot/debug/pilot_cli.go --type lds --proxytag httpbin --kubeconfig path/to/kube/config
// ```

package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	core "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	discovery "github.com/envoyproxy/go-control-plane/envoy/service/discovery/v3"
	"google.golang.org/grpc"
	v1 "k8s.io/api/core/v1"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"

	"google.golang.org/grpc/credentials"
	structpb "google.golang.org/protobuf/types/known/structpb"
	v3 "istio.io/istio/pilot/pkg/xds/v3"
	"istio.io/istio/pkg/env"
	"istio.io/istio/pkg/log"
	"istio.io/istio/pkg/util/protomarshal"
)

const (
	LocalPortStart = 50000
	LocalPortEnd   = 60000
)

// PodInfo holds information to identify pod.
type PodInfo struct {
	Name      string
	Namespace string
	IP        string
	ProxyType string
}

func getAllPods(kubeconfig string) (*v1.PodList, error) {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, err
	}
	return clientset.CoreV1().Pods(meta_v1.NamespaceAll).List(context.TODO(), meta_v1.ListOptions{})
}

func NewPodInfo(nameOrAppLabel string, kubeconfig string, proxyType string) *PodInfo {
	log.Infof("Using kube config at %s", kubeconfig)
	pods, err := getAllPods(kubeconfig)
	if err != nil {
		log.Errorf(err.Error())
		return nil
	}

	for _, pod := range pods.Items {
		log.Infof("pod %q", pod.Name)
		if pod.Name == nameOrAppLabel {
			log.Infof("Found pod %s.%s~%s matching name %q", pod.Name, pod.Namespace, pod.Status.PodIP, nameOrAppLabel)
			return &PodInfo{
				Name:      pod.Name,
				Namespace: pod.Namespace,
				IP:        pod.Status.PodIP,
				ProxyType: proxyType,
			}
		}
		if app, ok := pod.ObjectMeta.Labels["app"]; ok && app == nameOrAppLabel {
			log.Infof("Found pod %s.%s~%s matching app label %q", pod.Name, pod.Namespace, pod.Status.PodIP, nameOrAppLabel)
			return &PodInfo{
				Name:      pod.Name,
				Namespace: pod.Namespace,
				IP:        pod.Status.PodIP,
				ProxyType: proxyType,
			}
		}
		if istio, ok := pod.ObjectMeta.Labels["istio"]; ok && istio == nameOrAppLabel {
			log.Infof("Found pod %s.%s~%s matching app label %q", pod.Name, pod.Namespace, pod.Status.PodIP, nameOrAppLabel)
			return &PodInfo{
				Name:      pod.Name,
				Namespace: pod.Namespace,
				IP:        pod.Status.PodIP,
			}
		}
	}
	log.Warnf("Cannot find pod with name or app label matching %q in registry.", nameOrAppLabel)
	return nil
}

func (p PodInfo) makeNodeID() string {
	if p.ProxyType != "" {
		return fmt.Sprintf("%s~%s~%s.%s~%s.svc.cluster.local", p.ProxyType, p.IP, p.Name, p.Namespace, p.Namespace)
	}
	if strings.HasPrefix(p.Name, "istio-ingressgateway") || strings.HasPrefix(p.Name, "istio-egressgateway") {
		return fmt.Sprintf("router~%s~%s.%s~%s.svc.cluster.local", p.IP, p.Name, p.Namespace, p.Namespace)
	}
	if strings.HasPrefix(p.Name, "istio-ingress") {
		return fmt.Sprintf("ingress~%s~%s.%s~%s.svc.cluster.local", p.IP, p.Name, p.Namespace, p.Namespace)
	}
	return fmt.Sprintf("sidecar~%s~%s.%s~%s.svc.cluster.local", p.IP, p.Name, p.Namespace, p.Namespace)
}

func configTypeToTypeURL(configType string) string {
	switch configType {
	case "lds":
		return v3.ListenerType
	case "cds":
		return v3.ClusterType
	case "rds":
		return v3.RouteType
	case "eds":
		return v3.EndpointType
	case "sds":
		return v3.SecretType
	default:
		panic(fmt.Sprintf("Unknown type %s", configType))
	}
}

func (p PodInfo) makeRequest(configType string, nodeMetadata *structpb.Struct) *discovery.DiscoveryRequest {
	return &discovery.DiscoveryRequest{
		Node: &core.Node{
			Id:       p.makeNodeID(),
			Metadata: nodeMetadata,
		},
		TypeUrl: configTypeToTypeURL(configType),
	}
}

func (p PodInfo) appendResources(req *discovery.DiscoveryRequest, resources []string) *discovery.DiscoveryRequest {
	req.ResourceNames = resources
	return req
}

func (p PodInfo) getXdsResponse(pilotURL string, tlsConfig *tls.Config, req *discovery.DiscoveryRequest) (*discovery.DiscoveryResponse, error) {
	opts := []grpc.DialOption{}
	if tlsConfig != nil {
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	conn, err := grpc.Dial(pilotURL, opts...)
	if err != nil {
		panic(err.Error())
	}
	defer func() { _ = conn.Close() }()

	adsClient := discovery.NewAggregatedDiscoveryServiceClient(conn)
	stream, err := adsClient.StreamAggregatedResources(context.Background())
	if err != nil {
		panic(err.Error())
	}
	err = stream.Send(req)
	if err != nil {
		panic(err.Error())
	}
	res, err := stream.Recv()
	if err != nil {
		panic(err.Error())
	}
	return res, err
}

var homeVar = env.RegisterStringVar("HOME", "", "")

func resolveKubeConfigPath(kubeConfig string) string {
	path := strings.Replace(kubeConfig, "~", homeVar.Get(), 1)
	ret, err := filepath.Abs(path)
	if err != nil {
		panic(err.Error())
	}
	return ret
}

// nolint: golint
func portForwardPilot(kubeConfig, pilotURL string, secure bool) (*os.Process, string, error) {
	if pilotURL != "" {
		// No need to port-forward, url is already provided.
		return nil, pilotURL, nil
	}
	log.Info("Pilot url is not provided, try to port-forward pilot pod.")

	podName := ""
	pods, err := getAllPods(kubeConfig)
	if err != nil {
		return nil, "", err
	}
	for _, pod := range pods.Items {
		if app, ok := pod.ObjectMeta.Labels["app"]; ok && app == "istiod" {
			podName = pod.Name
		}
	}
	if podName == "" {
		return nil, "", fmt.Errorf("cannot find istio-pilot pod")
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	remotePort := 15010
	if secure {
		remotePort = 15012 // secure port
	}
	localPort := r.Intn(LocalPortEnd-LocalPortStart) + LocalPortStart
	cmd := fmt.Sprintf("kubectl port-forward %s -n istio-system %d:%d", podName, localPort, remotePort)
	parts := strings.Split(cmd, " ")
	c := exec.Command(parts[0], parts[1:]...)
	err = c.Start()
	if err != nil {
		return nil, "", err
	}
	// Make sure istio-pilot is reachable.
	reachable := false
	url := fmt.Sprintf("localhost:%d", localPort)
	for i := 0; i < 10; i++ {
		conn, err := net.Dial("tcp", url)
		if err == nil {
			_ = conn.Close()
			reachable = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	if !reachable {
		return nil, "", fmt.Errorf("cannot reach local pilot url: %s", url)
	}
	return c.Process, fmt.Sprintf("localhost:%d", localPort), nil
}

func main() {
	kubeConfig := flag.String("kubeconfig", "~/.kube/config", "path to the kubeconfig file. Default is ~/.kube/config")
	pilotURL := flag.String("pilot", "", "pilot address. Will try port forward if not provided.")
	configType := flag.String("type", "lds", "lds, cds, rds or eds. Default lds.")
	proxyType := flag.String("proxytype", "", "sidecar, ingress, router.")
	proxyTag := flag.String("proxytag", "", "Pod name or app label or istio label to identify the proxy.")
	resources := flag.String("res", "", "Resource(s) to get config for. LDS/CDS should leave it empty.")
	outputFile := flag.String("out", "", "output file. Leave blank to go to stdout")
	certFile := flag.String("cert", "", "Istio mTLS cert file to authenticate to pilot. If not provided, will use insecure connection.")
	keyFile := flag.String("key", "", "Istio mTLS key file to authenticate to pilot. If not provided, will use insecure connection.")
	istioVersion := flag.String("istioversion", "1.26.0", "Istio version.")
	clusterID := flag.String("clusterid", "Kubernetes", "Cluster ID.")
	flag.Parse()

	var tlsConfig *tls.Config
	if *certFile != "" && *keyFile != "" {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			log.Errorf("failed to load Istio mTLS certificate: %v", err)
			return
		}
		tlsConfig = &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true}
	}
	secure := tlsConfig != nil

	process, pilot, err := portForwardPilot(resolveKubeConfigPath(*kubeConfig), *pilotURL, secure)
	if err != nil {
		log.Errorf("pilot port forward failed: %v", err)
		return
	}
	defer func() {
		if process != nil {
			err := process.Kill()
			if err != nil {
				log.Errorf("Failed to kill port-forward process, pid: %d", process.Pid)
			}
		}
	}()
	pod := NewPodInfo(*proxyTag, resolveKubeConfigPath(*kubeConfig), *proxyType)

	nodeMetadata := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"ISTIO_VERSION": structpb.NewStringValue(*istioVersion),
			"CLUSTER_ID":    structpb.NewStringValue(*clusterID),
		},
	}

	var resp *discovery.DiscoveryResponse
	switch *configType {
	case "lds", "cds":
		resp, err = pod.getXdsResponse(pilot, tlsConfig, pod.makeRequest(*configType, nodeMetadata))
	case "rds", "eds", "sds":
		resp, err = pod.getXdsResponse(pilot, tlsConfig, pod.appendResources(pod.makeRequest(*configType, nodeMetadata), strings.Split(*resources, ",")))
	default:
		log.Errorf("Unknown config type: %q", *configType)
		os.Exit(1)
	}

	if err != nil {
		log.Errorf("Failed to get Xds response for %v. Error: %v", *resources, err)
		return
	}
	strResponse, err := protomarshal.ToJSONWithIndent(resp, " ")
	if err != nil {
		log.Errorf("Cannot format xDS response: %v", err)
		strResponse = resp.String()
	}
	if outputFile == nil || *outputFile == "" {
		fmt.Printf("%v\n", strResponse)
	} else if err := os.WriteFile(*outputFile, []byte(strResponse), 0o644); err != nil {
		log.Errorf("Cannot write output to file %q", *outputFile)
	}
}

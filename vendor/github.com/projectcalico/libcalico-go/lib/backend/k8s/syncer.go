// Copyright (c) 2016-2017 Tigera, Inc. All rights reserved.
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

package k8s

import (
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/projectcalico/libcalico-go/lib/backend/api"
	"github.com/projectcalico/libcalico-go/lib/backend/compat"
	"github.com/projectcalico/libcalico-go/lib/backend/k8s/resources"
	"github.com/projectcalico/libcalico-go/lib/backend/k8s/thirdparty"
	"github.com/projectcalico/libcalico-go/lib/backend/model"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sapi "k8s.io/client-go/pkg/api/v1"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	extensions "k8s.io/client-go/pkg/apis/extensions/v1beta1"
	"k8s.io/client-go/tools/cache"
)

type kubeAPI interface {
	NamespaceWatch(metav1.ListOptions) (watch.Interface, error)
	PodWatch(string, metav1.ListOptions) (watch.Interface, error)
	NetworkPolicyWatch(metav1.ListOptions) (watch.Interface, error)
	GlobalConfigWatch(metav1.ListOptions) (watch.Interface, error)
	IPPoolWatch(metav1.ListOptions) (watch.Interface, error)
	NodeWatch(metav1.ListOptions) (watch.Interface, error)
	NamespaceList(metav1.ListOptions) (*k8sapi.NamespaceList, error)
	NetworkPolicyList() (extensions.NetworkPolicyList, error)
	PodList(string, metav1.ListOptions) (*k8sapi.PodList, error)
	GlobalConfigList(model.GlobalConfigListOptions) ([]*model.KVPair, error)
	HostConfigList(model.HostConfigListOptions) ([]*model.KVPair, error)
	IPPoolList(l model.IPPoolListOptions) ([]*model.KVPair, error)
	NodeList(opts metav1.ListOptions) (list *k8sapi.NodeList, err error)
	getReadyStatus(k model.ReadyFlagKey) (*model.KVPair, error)
}

type realKubeAPI struct {
	kc *KubeClient
}

func (k *realKubeAPI) NamespaceWatch(opts metav1.ListOptions) (watch watch.Interface, err error) {
	watch, err = k.kc.clientSet.Namespaces().Watch(opts)
	return
}

func (k *realKubeAPI) PodWatch(namespace string, opts metav1.ListOptions) (watch watch.Interface, err error) {
	watch, err = k.kc.clientSet.Pods(namespace).Watch(opts)
	return
}

func (k *realKubeAPI) NetworkPolicyWatch(opts metav1.ListOptions) (watch watch.Interface, err error) {
	netpolListWatcher := cache.NewListWatchFromClient(
		k.kc.clientSet.Extensions().RESTClient(),
		"networkpolicies",
		"",
		fields.Everything())
	watch, err = netpolListWatcher.WatchFunc(opts)
	return
}

func (k *realKubeAPI) GlobalConfigWatch(opts metav1.ListOptions) (watch watch.Interface, err error) {
	globalConfigWatcher := cache.NewListWatchFromClient(
		k.kc.tprClient,
		"globalconfigs",
		"kube-system",
		fields.Everything())
	watch, err = globalConfigWatcher.WatchFunc(opts)
	return
}

func (k *realKubeAPI) IPPoolWatch(opts metav1.ListOptions) (watch watch.Interface, err error) {
	ipPoolWatcher := cache.NewListWatchFromClient(
		k.kc.tprClient,
		"ippools",
		"kube-system",
		fields.Everything())
	watch, err = ipPoolWatcher.WatchFunc(opts)
	return
}

func (k *realKubeAPI) NodeWatch(opts metav1.ListOptions) (watch watch.Interface, err error) {
	watch, err = k.kc.clientSet.Nodes().Watch(opts)
	return
}

func (k *realKubeAPI) NamespaceList(opts metav1.ListOptions) (list *k8sapi.NamespaceList, err error) {
	list, err = k.kc.clientSet.Namespaces().List(opts)
	return
}

func (k *realKubeAPI) NetworkPolicyList() (list extensions.NetworkPolicyList, err error) {
	list = extensions.NetworkPolicyList{}
	err = k.kc.clientSet.Extensions().RESTClient().
		Get().
		Resource("networkpolicies").
		Timeout(10 * time.Second).
		Do().Into(&list)
	return
}

func (k *realKubeAPI) PodList(namespace string, opts metav1.ListOptions) (list *k8sapi.PodList, err error) {
	list, err = k.kc.clientSet.Pods(namespace).List(opts)
	return
}

func (k *realKubeAPI) GlobalConfigList(l model.GlobalConfigListOptions) ([]*model.KVPair, error) {
	return k.kc.listGlobalConfig(l)
}

func (k *realKubeAPI) HostConfigList(l model.HostConfigListOptions) ([]*model.KVPair, error) {
	return k.kc.listHostConfig(l)
}

func (k *realKubeAPI) IPPoolList(l model.IPPoolListOptions) ([]*model.KVPair, error) {
	return k.kc.List(l)
}

func (k *realKubeAPI) NodeList(opts metav1.ListOptions) (list *k8sapi.NodeList, err error) {
	list, err = k.kc.clientSet.Nodes().List(opts)
	return
}

func (k *realKubeAPI) getReadyStatus(key model.ReadyFlagKey) (*model.KVPair, error) {
	return k.kc.getReadyStatus(key)
}

func newSyncer(kubeAPI kubeAPI, converter converter, callbacks api.SyncerCallbacks, disableNodePoll bool) *kubeSyncer {
	syn := &kubeSyncer{
		kubeAPI:         kubeAPI,
		converter:       converter,
		callbacks:       callbacks,
		tracker:         map[string]model.Key{},
		disableNodePoll: disableNodePoll,
		stopChan:        make(chan int),
	}
	return syn
}

type kubeSyncer struct {
	kubeAPI         kubeAPI
	converter       converter
	callbacks       api.SyncerCallbacks
	OneShot         bool
	tracker         map[string]model.Key
	disableNodePoll bool
	stopChan        chan int
}

// Holds resource version information.
type resourceVersions struct {
	nodeVersion          string
	podVersion           string
	namespaceVersion     string
	networkPolicyVersion string
	globalConfigVersion  string
	poolVersion          string
}

func (syn *kubeSyncer) Start() {
	// Start a background thread to read snapshots from and watch the Kubernetes API,
	// and pass updates via callbacks.
	go syn.readFromKubernetesAPI()
}

func (syn *kubeSyncer) Stop() {
	syn.stopChan <- 1
}

// sendUpdates sends updates to the callback and updates the resource
// tracker.
func (syn *kubeSyncer) sendUpdates(kvps []model.KVPair) {
	updates := syn.convertKVPairsToUpdates(kvps)

	// Send to the callback and update the tracker.
	syn.callbacks.OnUpdates(updates)
	syn.updateTracker(updates)
}

// convertKVPairsToUpdates converts a list of KVPairs to the list
// of api.Update objects which should be sent to OnUpdates.  It filters out
// deletes for any KVPairs which we don't know about.
func (syn *kubeSyncer) convertKVPairsToUpdates(kvps []model.KVPair) []api.Update {
	updates := []api.Update{}
	for _, kvp := range kvps {
		if _, ok := syn.tracker[kvp.Key.String()]; !ok && kvp.Value == nil {
			// The given KVPair is not in the tracker, and is a delete, so no need to
			// send a delete update.
			continue
		}
		updates = append(updates, api.Update{KVPair: kvp, UpdateType: syn.getUpdateType(kvp)})
	}
	return updates
}

// updateTracker updates the global object tracker with the given update.
// updateTracker should be called after sending a update to the OnUpdates callback.
func (syn *kubeSyncer) updateTracker(updates []api.Update) {
	for _, upd := range updates {
		if upd.UpdateType == api.UpdateTypeKVDeleted {
			log.Debugf("Delete from tracker: %+v", upd.KVPair.Key)
			delete(syn.tracker, upd.KVPair.Key.String())
		} else {
			log.Debugf("Update tracker: %+v: %+v", upd.KVPair.Key, upd.KVPair.Revision)
			syn.tracker[upd.KVPair.Key.String()] = upd.KVPair.Key
		}
	}
}

func (syn *kubeSyncer) getUpdateType(kvp model.KVPair) api.UpdateType {
	if kvp.Value == nil {
		// If the value is nil, then this is a delete.
		return api.UpdateTypeKVDeleted
	}

	// Not a delete.
	if _, ok := syn.tracker[kvp.Key.String()]; !ok {
		// If not a delete and it does not exist in the tracker, this is an add.
		return api.UpdateTypeKVNew
	} else {
		// If not a delete and it exists in the tracker, this is an update.
		return api.UpdateTypeKVUpdated
	}
}

func (syn *kubeSyncer) readFromKubernetesAPI() {
	log.Info("Starting Kubernetes API read worker")

	// Keep track of the latest resource versions.
	latestVersions := resourceVersions{}

	// Other watcher vars.
	var nsChan, poChan, npChan, gcChan, poolChan, noChan <-chan watch.Event
	var event watch.Event
	var kvp *model.KVPair
	var opts metav1.ListOptions
	var openWatchers []watch.Interface
	closeWatchers := func() {
		for _, w := range openWatchers {
			log.WithField("watcher", w).Debug("Closing old watcher.")
			w.Stop()
		}
		openWatchers = nil
	}

	// Always perform an initial snapshot.
	needsResync := true

	log.Info("Starting Kubernetes API read loop")
	for {
		// If we need to resync, do so.
		if needsResync {
			// Set status to ResyncInProgress.
			log.Debugf("Resync required - latest versions: %+v", latestVersions)
			syn.callbacks.OnStatusUpdated(api.ResyncInProgress)

			// Get snapshot from datastore.
			snap, existingKeys, latestVersions := syn.performSnapshot()
			log.Debugf("Snapshot: %+v, keys: %+v, versions: %+v", snap, existingKeys, latestVersions)

			// Go through and delete anything that existed before, but doesn't anymore.
			syn.performSnapshotDeletes(existingKeys)

			// Send the snapshot through.
			syn.sendUpdates(snap)

			log.Debugf("Snapshot complete - start watch from %+v", latestVersions)
			syn.callbacks.OnStatusUpdated(api.InSync)

			// Don't start watches if we're in oneshot mode.
			if syn.OneShot {
				log.Info("OneShot mode, do not start watches")
				return
			}

			// Close the previous crop of watchers to avoid leaking resources when we
			// recreate them below.
			closeWatchers()

			// Create the Kubernetes API watchers.
			opts = metav1.ListOptions{ResourceVersion: latestVersions.namespaceVersion}
			nsWatch, err := syn.kubeAPI.NamespaceWatch(opts)
			if err != nil {
				log.Warn("Failed to watch Namespaces, retrying: %s", err)
				time.Sleep(1 * time.Second)
				continue
			}
			openWatchers = append(openWatchers, nsWatch)
			opts = metav1.ListOptions{ResourceVersion: latestVersions.podVersion}
			poWatch, err := syn.kubeAPI.PodWatch("", opts)
			if err != nil {
				log.Warn("Failed to watch Pods, retrying: %s", err)
				time.Sleep(1 * time.Second)
				continue
			}
			openWatchers = append(openWatchers, poWatch)

			// Create watcher for NetworkPolicy objects.
			opts = metav1.ListOptions{ResourceVersion: latestVersions.networkPolicyVersion}
			npWatch, err := syn.kubeAPI.NetworkPolicyWatch(opts)
			if err != nil {
				log.Warnf("Failed to watch NetworkPolicies, retrying: %s", err)
				time.Sleep(1 * time.Second)
				continue
			}
			openWatchers = append(openWatchers, npWatch)

			// Create watcher for Calico global config resources.
			globalConfigWatch, err := syn.kubeAPI.GlobalConfigWatch(opts)
			if err != nil {
				log.Warnf("Failed to watch GlobalConfig, retrying: %s", err)
				time.Sleep(1 * time.Second)
				continue
			}
			openWatchers = append(openWatchers, globalConfigWatch)

			// Watcher for Calico IP Pool resources.
			ipPoolWatch, err := syn.kubeAPI.IPPoolWatch(opts)
			if err != nil {
				log.Warnf("Failed to watch IPPools, retrying: %s", err)
				time.Sleep(1 * time.Second)
				continue
			}
			openWatchers = append(openWatchers, ipPoolWatch)

			if !syn.disableNodePoll {
				// Create watcher for Node objects
				opts := metav1.ListOptions{ResourceVersion: latestVersions.nodeVersion}
				nodeWatch, err := syn.kubeAPI.NodeWatch(opts)
				if err != nil {
					log.Warnf("Failed to watch Nodes, retrying: %s", err)
					time.Sleep(1 * time.Second)
					continue
				}
				openWatchers = append(openWatchers, nodeWatch)
				noChan = nodeWatch.ResultChan()
			}
			nsChan = nsWatch.ResultChan()
			poChan = poWatch.ResultChan()
			npChan = npWatch.ResultChan()
			gcChan = globalConfigWatch.ResultChan()
			poolChan = ipPoolWatch.ResultChan()

			// Success - reset the flag.
			needsResync = false
		}

		// Select on the various watch channels.
		select {
		case <-syn.stopChan:
			log.Info("Syncer told to stop reading")
			closeWatchers()
			return
		case event = <-nsChan:
			log.Debugf("Incoming Namespace watch event. Type=%s", event.Type)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out into the sync loop.
				log.Warnf("Event triggered resync: %+v", event)
				continue
			}

			// Event is OK - parse it.
			kvps := syn.parseNamespaceEvent(event)
			latestVersions.namespaceVersion = kvps[0].Revision.(string)
			syn.sendUpdates(kvps)
			continue
		case event = <-poChan:
			log.Debugf("Incoming Pod watch event. Type=%s", event.Type)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out into the sync loop.
				log.Warnf("Event triggered resync: %+v", event)
				continue
			}

			// Event is OK - parse it.
			if kvp = syn.parsePodEvent(event); kvp != nil {
				// Only send the update if we care about it.  We filter
				// out a number of events that aren't useful for us.
				latestVersions.podVersion = kvp.Revision.(string)
				syn.sendUpdates([]model.KVPair{*kvp})
			}
		case event = <-npChan:
			log.Debugf("Incoming NetworkPolicy watch event. Type=%s", event.Type)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out into the sync loop.
				log.Warnf("Event triggered resync: %+v", event)
				continue
			}

			// Event is OK - parse it and send it over the channel.
			kvp = syn.parseNetworkPolicyEvent(event)
			latestVersions.networkPolicyVersion = kvp.Revision.(string)
			syn.sendUpdates([]model.KVPair{*kvp})
		case event = <-gcChan:
			log.Debugf("Incoming GlobalConfig watch event. Type=%s", event.Type)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out into the sync loop.
				log.Warnf("Event triggered resync: %+v", event)
				continue
			}

			// Event is OK - parse it and send it over the channel.
			kvp = syn.parseGlobalConfigEvent(event)
			latestVersions.globalConfigVersion = kvp.Revision.(string)
			syn.sendUpdates([]model.KVPair{*kvp})
		case event = <-poolChan:
			log.Debugf("Incoming IPPool watch event. Type=%s", event.Type)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out into the sync loop.
				log.Warnf("Event triggered resync: %+v", event)
				continue
			}
			// Event is OK - parse it and send it over the channel.
			kvp = syn.parseIPPoolEvent(event)
			latestVersions.poolVersion = kvp.Revision.(string)
			syn.sendUpdates([]model.KVPair{*kvp})
		case event = <-noChan:
			log.Debugf("Incoming Node watch event. Type=%s", event.Type)
			if needsResync = syn.eventTriggersResync(event); needsResync {
				// We need to resync.  Break out of the sync loop.
				log.Warnf("Event triggered resync: %+v", event)
				continue
			}

			// Event is OK - parse it and send it over the channel.
			kvp = syn.parseNodeEvent(event)
			latestVersions.nodeVersion = kvp.Revision.(string)
			syn.sendUpdates([]model.KVPair{*kvp})
		}
	}
}

func (syn *kubeSyncer) performSnapshotDeletes(exists map[string]bool) {
	log.Info("Checking for any deletes for snapshot")
	deletes := []model.KVPair{}
	log.Debugf("Keys in snapshot: %+v", exists)
	for cachedKey, k := range syn.tracker {
		// Check each cached key to see if it exists in the snapshot.  If it doesn't,
		// we need to send a delete for it.
		if _, stillExists := exists[cachedKey]; !stillExists {
			log.Debugf("Cached key not in snapshot: %+v", cachedKey)
			deletes = append(deletes, model.KVPair{Key: k, Value: nil})
		}
	}
	log.Infof("Sending snapshot deletes: %+v", deletes)
	syn.sendUpdates(deletes)
}

// performSnapshot returns a list of existing objects in the datastore,
// a mapping of model.Key objects representing the objects which exist in the datastore, and
// populates the provided resourceVersions with the latest k8s resource version
// for each.
func (syn *kubeSyncer) performSnapshot() ([]model.KVPair, map[string]bool, resourceVersions) {
	opts := metav1.ListOptions{}
	versions := resourceVersions{}
	var snap []model.KVPair
	var keys map[string]bool

	// Loop until we successfully are able to accesss the API.
	for {
		// Initialize the values to return.
		snap = []model.KVPair{}
		keys = map[string]bool{}

		// Get Namespaces (Profiles)
		log.Info("Syncing Namespaces")
		nsList, err := syn.kubeAPI.NamespaceList(opts)
		if err != nil {
			log.Warnf("Error syncing Namespaces, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Info("Received Namespace List() response")

		versions.namespaceVersion = nsList.ListMeta.ResourceVersion
		for _, ns := range nsList.Items {
			// The Syncer API expects a profile to be broken into its underlying
			// components - rules, tags, labels.
			profile, err := syn.converter.namespaceToProfile(&ns)
			if err != nil {
				log.Panicf("%s", err)
			}
			rules, tags, labels := compat.ToTagsLabelsRules(profile)
			rules.Revision = profile.Revision
			tags.Revision = profile.Revision
			labels.Revision = profile.Revision

			snap = append(snap, *rules, *tags, *labels)
			keys[rules.Key.String()] = true
			keys[tags.Key.String()] = true
			keys[labels.Key.String()] = true
		}

		// Get NetworkPolicies (Policies)
		log.Info("Syncing NetworkPolicy")
		npList, err := syn.kubeAPI.NetworkPolicyList()
		if err != nil {
			log.Warnf("Error querying NetworkPolicies during snapshot, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Info("Received NetworkPolicy List() response")

		versions.networkPolicyVersion = npList.ListMeta.ResourceVersion
		for _, np := range npList.Items {
			pol, _ := syn.converter.networkPolicyToPolicy(&np)
			snap = append(snap, *pol)
			keys[pol.Key.String()] = true
		}

		// Get Pods (WorkloadEndpoints)
		log.Info("Syncing Pods")
		poList, err := syn.kubeAPI.PodList("", opts)
		if err != nil {
			log.Warnf("Error querying Pods during snapshot, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Info("Received Pod List() response")

		versions.podVersion = poList.ListMeta.ResourceVersion
		for _, po := range poList.Items {
			// Ignore any updates for pods which are not ready / valid.
			if !syn.converter.isReadyCalicoPod(&po) {
				log.Debugf("Skipping pod %s/%s", po.ObjectMeta.Namespace, po.ObjectMeta.Name)
				continue
			}

			// Convert to a workload endpoint.
			wep, err := syn.converter.podToWorkloadEndpoint(&po)
			if err != nil {
				log.WithError(err).Error("Failed to convert pod to workload endpoint")
				continue
			}
			snap = append(snap, *wep)
			keys[wep.Key.String()] = true
		}

		// Sync GlobalConfig.
		log.Info("Syncing GlobalConfig")
		confList, err := syn.kubeAPI.GlobalConfigList(model.GlobalConfigListOptions{})
		if err != nil {
			log.Warnf("Error querying GlobalConfig during snapshot, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Info("Received GlobalConfig List() response")

		for _, c := range confList {
			snap = append(snap, *c)
			keys[c.Key.String()] = true
		}

		// Sync Hostconfig.
		log.Info("Syncing HostConfig")
		hostConfList, err := syn.kubeAPI.HostConfigList(model.HostConfigListOptions{})
		if err != nil {
			log.Warnf("Error querying HostConfig during snapshot, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Info("Received HostConfig List() response")

		for _, h := range hostConfList {
			snap = append(snap, *h)
			keys[h.Key.String()] = true
		}

		// Sync IP Pools.
		log.Info("Syncing IP Pools")
		poolList, err := syn.kubeAPI.IPPoolList(model.IPPoolListOptions{})
		if err != nil {
			log.Warnf("Error querying IP Pools during snapshot, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		log.Info("Received IP Pools List() response")

		for _, p := range poolList {
			snap = append(snap, *p)
			keys[p.Key.String()] = true
		}

		if !syn.disableNodePoll {
			log.Info("Syncing Nodes")
			noList, err := syn.kubeAPI.NodeList(opts)
			if err != nil {
				log.Warnf("Error syncing Nodes, retrying: %s", err)
				time.Sleep(1 * time.Second)
				continue
			}
			log.Info("Received Node List() response")

			versions.nodeVersion = noList.ListMeta.ResourceVersion
			for _, no := range noList.Items {
				node, err := resources.K8sNodeToCalico(&no)
				if err != nil {
					log.Panicf("%s", err)
				}
				if node != nil {
					snap = append(snap, *node)
					keys[node.Key.String()] = true
				}
			}
		}

		// Include ready state.
		ready, err := syn.kubeAPI.getReadyStatus(model.ReadyFlagKey{})
		if err != nil {
			log.Warnf("Error querying ready status during snapshot, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}
		snap = append(snap, *ready)
		keys[ready.Key.String()] = true

		log.Infof("Snapshot resourceVersions: %+v", versions)
		log.Debugf("Created snapshot: %+v", snap)
		return snap, keys, versions
	}
}

// eventTriggersResync returns true of the given event requires a
// full datastore resync to occur, and false otherwise.
func (syn *kubeSyncer) eventTriggersResync(e watch.Event) bool {
	// If we encounter an error, or if the event is nil (which can indicate
	// an unexpected connection close).
	if e.Type == watch.Error || e.Object == nil {
		log.Warnf("Event requires snapshot: %+v", e)
		return true
	}
	return false
}

func (syn *kubeSyncer) parseNamespaceEvent(e watch.Event) []model.KVPair {
	ns, ok := e.Object.(*k8sapi.Namespace)
	if !ok {
		log.Panicf("Invalid namespace event: %+v", e.Object)
	}

	// Convert the received Namespace into a profile KVPair.
	profile, err := syn.converter.namespaceToProfile(ns)
	if err != nil {
		log.Panicf("%s", err)
	}
	rules, tags, labels := compat.ToTagsLabelsRules(profile)
	rules.Revision = profile.Revision
	tags.Revision = profile.Revision
	labels.Revision = profile.Revision

	// For deletes, we need to nil out the Value part of the KVPair.
	if e.Type == watch.Deleted {
		rules.Value = nil
		tags.Value = nil
		labels.Value = nil
	}

	// Return the updates.
	return []model.KVPair{*rules, *tags, *labels}
}

func (syn *kubeSyncer) parseNodeEvent(e watch.Event) *model.KVPair {
	node, ok := e.Object.(*k8sapi.Node)
	if !ok {
		log.Panicf("Invalid node event. Type: %s, Object: %+v", e.Type, e.Object)
	}

	kvp, err := resources.K8sNodeToCalico(node)
	if err != nil {
		log.Panicf("%s", err)
	}

	kvpHostIp := &model.KVPair{
		Key:      model.HostIPKey{Hostname: node.Name},
		Value:    kvp.Value.(*model.Node).BGPIPv4Addr,
		Revision: kvp.Revision,
	}

	if e.Type == watch.Deleted {
		kvp.Value = nil
	}

	return kvpHostIp
}

// parsePodEvent returns a KVPair for the given event.  If the event isn't
// useful, parsePodEvent returns nil to indicate that there is nothing to do.
func (syn *kubeSyncer) parsePodEvent(e watch.Event) *model.KVPair {
	pod, ok := e.Object.(*k8sapi.Pod)
	if !ok {
		log.Panicf("Invalid pod event. Type: %s, Object: %+v", e.Type, e.Object)
	}

	switch e.Type {
	case watch.Deleted:
		// For deletes, the validity conditions are different.  We only care if the update
		// is not for a host-networked Pods, but don't care about IP / scheduled state.
		if syn.converter.isHostNetworked(pod) {
			log.WithField("pod", pod.Name).Debug("Pod is host networked.")
			log.Debugf("Skipping delete for pod %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
			return nil
		}
	default:
		// Ignore add/modify updates for Pods that shouldn't be shown in the Calico API.
		// e.g host networked Pods, or Pods that don't yet have an IP address.
		if !syn.converter.isReadyCalicoPod(pod) {
			log.Debugf("Skipping add/modify for pod %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
			return nil
		}
	}

	// Convert the received Pod into a KVPair.
	kvp, err := syn.converter.podToWorkloadEndpoint(pod)
	if err != nil {
		// If we fail to parse, then ignore this update and emit a log.
		log.WithField("error", err).Error("Failed to parse Pod event")
		return nil
	}

	// We behave differently based on the event type.
	switch e.Type {
	case watch.Deleted:
		// For deletes, we need to nil out the Value part of the KVPair.
		log.Debugf("Delete for pod %s/%s", pod.ObjectMeta.Namespace, pod.ObjectMeta.Name)
		kvp.Value = nil
	}

	return kvp
}

func (syn *kubeSyncer) parseNetworkPolicyEvent(e watch.Event) *model.KVPair {
	log.Debug("Parsing NetworkPolicy watch event")
	// First, check the event type.
	np, ok := e.Object.(*extensions.NetworkPolicy)
	if !ok {
		log.Panicf("Invalid NetworkPolicy event. Type: %s, Object: %+v", e.Type, e.Object)
	}

	// Convert the received NetworkPolicy into a profile KVPair.
	kvp, err := syn.converter.networkPolicyToPolicy(np)
	if err != nil {
		log.Panicf("%s", err)
	}

	// For deletes, we need to nil out the Value part of the KVPair
	if e.Type == watch.Deleted {
		kvp.Value = nil
	}
	return kvp
}

func (syn *kubeSyncer) parseGlobalConfigEvent(e watch.Event) *model.KVPair {
	log.Debug("Parsing GlobalConfig watch event")
	// First, check the event type.
	gc, ok := e.Object.(*custom.GlobalConfig)
	if !ok {
		log.Panicf("Invalid GlobalConfig event. Type: %s, Object: %+v", e.Type, e.Object)
	}

	// Convert the received GlobalConfig into a KVPair.
	kvp := syn.converter.tprToGlobalConfig(gc)

	// For deletes, we need to nil out the Value part of the KVPair
	if e.Type == watch.Deleted {
		kvp.Value = nil
	}
	return kvp
}

func (syn *kubeSyncer) parseIPPoolEvent(e watch.Event) *model.KVPair {
	log.Debug("Parsing IPPool watch event")
	// First, check the event type.
	tpr, ok := e.Object.(*custom.IpPool)
	if !ok {
		log.Panicf("Invalid IPPool event. Type: %s, Object: %+v", e.Type, e.Object)
	}

	// Convert the received IPPool into a KVPair.
	kvp := resources.ThirdPartyToIPPool(tpr)

	// For deletes, we need to nil out the Value part of the KVPair
	if e.Type == watch.Deleted {
		kvp.Value = nil
	}
	return kvp
}

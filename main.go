package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"

	yaml "gopkg.in/yaml.v2"

	"github.com/ericchiang/k8s"

	"github.com/deckarep/gosx-notifier"
)

func main() {

	client, err := loadClient("/Users/gunjan/.kube/config")
	if err != nil {
		log.Fatal(err)
	}

	pods, err := client.CoreV1().ListPods(context.Background(), k8s.AllNamespaces)
	if err != nil {
		log.Println(err)
	}
	for _, pods := range pods.Items {
		fmt.Printf("name=%s\n", *pods.Metadata.Name)
		podErrNotifier(*pods.Metadata.Name, "CrashLoop")
	}

}

func podErrNotifier(podName, state string) {
	//At a minimum specifiy a message to display to end-user.
	note := gosxnotifier.NewNotification("Check your Apple Stock!")

	//Optionally, set a title
	note.Title = "Pod Error 😱"

	//Optionally, set a subtitle
	note.Subtitle = podName + ":" + state

	//Optionally, set a sound from a predefined set.
	note.Sound = gosxnotifier.Basso

	//Optionally, set a group which ensures only one notification is ever shown replacing previous notification of same group id.
	note.Group = "com.unique.yourapp.identifier"

	//Optionally, specifiy a url or bundleid to open should the notification be
	//clicked.
	note.Link = fmt.Sprintf(`http://192.168.99.101:30000/#!/log/kube-system/%s/kubernetes-dashboard?namespace=_all`, podName) //or BundleID like: com.apple.Terminal

	//Optionally, an app icon (10.9+ ONLY)
	note.AppIcon = "/Users/gunjan/go/src/github.com/gunjan5/KubeNotify/icons/icon.png"

	//Optionally, a content image (10.9+ ONLY)
	note.ContentImage = "/Users/gunjan/go/src/github.com/gunjan5/KubeNotify/icons/icon.png"

	//Then, push the notification
	err := note.Push()

	if err != nil {
		log.Printf("Error pushing notification: %v", note)
	}
}

// loadClient parses a kubeconfig from a file and returns a Kubernetes
// client. It does not support extensions or client auth providers.
func loadClient(kubeconfigPath string) (*k8s.Client, error) {
	data, err := ioutil.ReadFile(kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("read kubeconfig: %v", err)
	}

	// Unmarshal YAML into a Kubernetes config object.
	var config k8s.Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("unmarshal kubeconfig: %v", err)
	}

	return k8s.NewClient(&config)
}

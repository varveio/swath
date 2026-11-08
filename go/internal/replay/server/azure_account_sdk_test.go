// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// versionTransport sends the SDK's requests with a service version the
// route answers: the Go SDK has no option for it, and keeps its x-ms-*
// names in lower case, so every spelling is replaced.
type versionTransport struct{}

func (versionTransport) Do(req *http.Request) (*http.Response, error) {
	for k := range req.Header {
		if strings.EqualFold(k, "x-ms-version") {
			delete(req.Header, k)
		}
	}
	req.Header["x-ms-version"] = []string{"2026-10-06"}
	return http.DefaultTransport.RoundTrip(req)
}

// TestAzureAccountInformationSDK reads Get Account Information through the
// Azure SDK's container and service clients, flat and HNS: the container
// client sends it on the container path and the service client on the
// ServiceEndpoint, /{account}/.
func TestAzureAccountInformationSDK(t *testing.T) {
	for _, hns := range []bool{false, true} {
		cfg := azureConfig("s3", "azure")
		cfg.AzureHNS = hns
		s := start(t, cfg)
		client, err := azblob.NewClientWithNoCredential(fmt.Sprintf("http://127.0.0.1:%d/replay/", s.Port()),
			&azblob.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: versionTransport{}, Retry: policy.RetryOptions{MaxRetries: -1}}})
		if err != nil {
			t.Fatal(err)
		}
		ctr, err := client.ServiceClient().NewContainerClient("bucket").GetAccountInfo(context.Background(), nil)
		if err != nil || ctr.IsHierarchicalNamespaceEnabled == nil || *ctr.IsHierarchicalNamespaceEnabled != hns ||
			ctr.AccountKind == nil || *ctr.AccountKind != "StorageV2" || ctr.SKUName == nil || *ctr.SKUName != "Standard_LRS" {
			t.Fatalf("hns=%v container client: %+v %v", hns, ctr, err)
		}
		svc, err := client.ServiceClient().GetAccountInfo(context.Background(), nil)
		if err != nil || svc.IsHierarchicalNamespaceEnabled == nil || *svc.IsHierarchicalNamespaceEnabled != hns {
			t.Fatalf("hns=%v service client: %+v %v", hns, svc, err)
		}
		if _, err := client.ServiceClient().NewContainerClient("other").GetAccountInfo(context.Background(), nil); err == nil ||
			!strings.Contains(err.Error(), "ContainerNotFound") {
			t.Fatalf("hns=%v other container: %v", hns, err)
		}
	}
}

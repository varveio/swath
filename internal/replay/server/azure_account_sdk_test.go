// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package server_test

import (
	"context"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"

	"github.com/varveio/swath/internal/source/azblob/azblobtest"
)

// TestAzureAccountInformationSDK reads Get Account Information through the
// Azure SDK's container and service clients, flat and HNS: the container
// client sends it on a container path (the served container or any other,
// which Azure answers alike) and the service client on the
// ServiceEndpoint, /{account}/. azblobtest.PinVersion sends a service
// version the replay answers.
func TestAzureAccountInformationSDK(t *testing.T) {
	for _, hns := range []bool{false, true} {
		var opts []azblobtest.Option
		if hns {
			opts = append(opts, azblobtest.HNS())
		}
		r := azblobtest.Serve(t, []string{"a", "b/c"}, opts...)
		client, err := azblob.NewClientWithNoCredential(r.Endpoint+"/",
			&azblob.ClientOptions{ClientOptions: azcore.ClientOptions{Transport: azblobtest.PinVersion{}, Retry: policy.RetryOptions{MaxRetries: -1}}})
		if err != nil {
			t.Fatal(err)
		}
		for _, container := range []string{r.Container, "no-such-container"} {
			got, err := client.ServiceClient().NewContainerClient(container).GetAccountInfo(context.Background(), nil)
			if err != nil || got.IsHierarchicalNamespaceEnabled == nil || *got.IsHierarchicalNamespaceEnabled != hns ||
				got.AccountKind == nil || *got.AccountKind != "StorageV2" || got.SKUName == nil || *got.SKUName != "Standard_LRS" ||
				got.Version == nil || *got.Version != azblobtest.SDKVersion {
				t.Fatalf("hns=%v container client on %s: %+v %v", hns, container, got, err)
			}
		}
		svc, err := client.ServiceClient().GetAccountInfo(context.Background(), nil)
		if err != nil || svc.IsHierarchicalNamespaceEnabled == nil || *svc.IsHierarchicalNamespaceEnabled != hns {
			t.Fatalf("hns=%v service client: %+v %v", hns, svc, err)
		}
	}
}

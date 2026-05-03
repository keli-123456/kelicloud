package azure

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/komari-monitor/komari/utils/outboundproxy"
)

const (
	defaultAuthorityURL  = "https://login.microsoftonline.com"
	defaultManagementURL = "https://management.azure.com"

	resourceAPIVersion     = "2021-04-01"
	subscriptionAPIVersion = "2022-12-01"
	computeSKUAPIVersion   = "2021-07-01"
	computeAPIVersion      = "2024-11-01"
	networkAPIVersion      = "2024-07-01"

	azureVNetIPv4CIDR   = "10.217.0.0/16"
	azureSubnetIPv4CIDR = "10.217.0.0/24"
	azureVNetIPv6CIDR   = "fd12:3456:789a::/48"
	azureSubnetIPv6CIDR = "fd12:3456:789a:1::/64"
)

type APIError struct {
	StatusCode int    `json:"-"`
	Code       string `json:"code,omitempty"`
	Message    string `json:"message,omitempty"`
}

func (e *APIError) Error() string {
	message := strings.TrimSpace(e.Message)
	if message == "" {
		message = http.StatusText(e.StatusCode)
	}
	if code := strings.TrimSpace(e.Code); code != "" {
		return code + ": " + message
	}
	if message != "" {
		return message
	}
	return "azure api request failed"
}

type Client struct {
	credential    CredentialRecord
	authorityURL  string
	managementURL string
	httpClient    *http.Client

	mu          sync.Mutex
	accessToken string
	tokenExpiry time.Time
}

type Subscription struct {
	ID                  string `json:"id"`
	SubscriptionID      string `json:"subscriptionId"`
	DisplayName         string `json:"displayName"`
	State               string `json:"state"`
	TenantID            string `json:"tenantId"`
	AuthorizationSource string `json:"authorizationSource"`
}

type Location struct {
	Name                string `json:"name"`
	DisplayName         string `json:"displayName"`
	RegionalDisplayName string `json:"regionalDisplayName"`
}

type VMSku struct {
	Name      string   `json:"name"`
	VCPUs     int      `json:"vcpus"`
	MemoryGB  float64  `json:"memory_gb"`
	Zones     []string `json:"zones"`
	MaxDataGB int      `json:"max_data_disk_count"`
}

type ImageReference struct {
	Publisher string `json:"publisher,omitempty"`
	Offer     string `json:"offer,omitempty"`
	SKU       string `json:"sku,omitempty"`
	Version   string `json:"version,omitempty"`
}

type CreateVirtualMachineRequest struct {
	Name          string
	ResourceGroup string
	Location      string
	Size          string
	AdminUsername string
	AdminPassword string
	SSHPublicKey  string
	UserData      string
	PublicIP      bool
	AssignIPv6    bool
	Image         ImageReference
	Tags          map[string]string
}

type Instance struct {
	InstanceID        string            `json:"instance_id"`
	ResourceID        string            `json:"resource_id"`
	Name              string            `json:"name"`
	ResourceGroup     string            `json:"resource_group"`
	Location          string            `json:"location"`
	Size              string            `json:"size"`
	ProvisioningState string            `json:"provisioning_state"`
	PowerState        string            `json:"power_state"`
	ComputerName      string            `json:"computer_name"`
	OsType            string            `json:"os_type"`
	Image             string            `json:"image"`
	PrivateIPs        []string          `json:"private_ips"`
	PublicIPs         []string          `json:"public_ips"`
	Tags              map[string]string `json:"tags"`
}

type NetworkInterface struct {
	ID                     string   `json:"id"`
	Name                   string   `json:"name"`
	Primary                bool     `json:"primary"`
	PrivateIPs             []string `json:"private_ips"`
	PublicIPs              []string `json:"public_ips"`
	SubnetIDs              []string `json:"subnet_ids"`
	NetworkSecurityGroupID string   `json:"network_security_group_id"`
}

type Disk struct {
	ID                 string `json:"id"`
	Name               string `json:"name"`
	Lun                int    `json:"lun,omitempty"`
	SizeGB             int    `json:"size_gb,omitempty"`
	CreateOption       string `json:"create_option,omitempty"`
	StorageAccountType string `json:"storage_account_type,omitempty"`
}

type InstanceDetail struct {
	Instance          Instance           `json:"instance"`
	VMID              string             `json:"vm_id"`
	Zones             []string           `json:"zones"`
	LicenseType       string             `json:"license_type"`
	NetworkInterfaces []NetworkInterface `json:"network_interfaces"`
	OSDisk            *Disk              `json:"os_disk,omitempty"`
	DataDisks         []Disk             `json:"data_disks"`
}

type PublicIPReplacementResult struct {
	OldPublicIPID string `json:"old_public_ip_id,omitempty"`
	OldPublicIP   string `json:"old_public_ip,omitempty"`
	NewPublicIPID string `json:"new_public_ip_id,omitempty"`
	NewPublicIP   string `json:"new_public_ip,omitempty"`
}

type DeleteVirtualMachineResult struct {
	DeletedResourceIDs []string `json:"deleted_resource_ids,omitempty"`
	CleanupErrors      []string `json:"cleanup_errors,omitempty"`
}

type virtualMachineResource struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Location   string            `json:"location"`
	Tags       map[string]string `json:"tags"`
	Zones      []string          `json:"zones"`
	Properties struct {
		VMID              string `json:"vmId"`
		ProvisioningState string `json:"provisioningState"`
		LicenseType       string `json:"licenseType"`
		HardwareProfile   struct {
			VMSize string `json:"vmSize"`
		} `json:"hardwareProfile"`
		StorageProfile struct {
			ImageReference struct {
				Publisher               string `json:"publisher"`
				Offer                   string `json:"offer"`
				SKU                     string `json:"sku"`
				Version                 string `json:"version"`
				ID                      string `json:"id"`
				SharedGalleryImageID    string `json:"sharedGalleryImageId"`
				CommunityGalleryImageID string `json:"communityGalleryImageId"`
			} `json:"imageReference"`
			OSDisk *struct {
				OSType       string `json:"osType"`
				Name         string `json:"name"`
				DiskSizeGB   int    `json:"diskSizeGB"`
				CreateOption string `json:"createOption"`
				ManagedDisk  *struct {
					ID                 string `json:"id"`
					StorageAccountType string `json:"storageAccountType"`
				} `json:"managedDisk"`
			} `json:"osDisk"`
			DataDisks []struct {
				Lun          int    `json:"lun"`
				Name         string `json:"name"`
				DiskSizeGB   int    `json:"diskSizeGB"`
				CreateOption string `json:"createOption"`
				ManagedDisk  *struct {
					ID                 string `json:"id"`
					StorageAccountType string `json:"storageAccountType"`
				} `json:"managedDisk"`
			} `json:"dataDisks"`
		} `json:"storageProfile"`
		OSProfile *struct {
			ComputerName string `json:"computerName"`
		} `json:"osProfile"`
		NetworkProfile struct {
			NetworkInterfaces []struct {
				ID         string `json:"id"`
				Properties struct {
					Primary bool `json:"primary"`
				} `json:"properties"`
			} `json:"networkInterfaces"`
		} `json:"networkProfile"`
		InstanceView *struct {
			Statuses []instanceStatus `json:"statuses"`
		} `json:"instanceView"`
	} `json:"properties"`
}

type instanceView struct {
	Statuses []instanceStatus `json:"statuses"`
}

type instanceStatus struct {
	Code          string `json:"code"`
	DisplayStatus string `json:"displayStatus"`
}

type networkInterfaceResource struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Tags       map[string]string `json:"tags"`
	Properties struct {
		NetworkSecurityGroup *struct {
			ID string `json:"id"`
		} `json:"networkSecurityGroup"`
		IPConfigurations []struct {
			Name       string `json:"name"`
			Properties struct {
				Primary          bool   `json:"primary"`
				PrivateIPAddress string `json:"privateIPAddress"`
				Subnet           *struct {
					ID string `json:"id"`
				} `json:"subnet"`
				PublicIPAddress *struct {
					ID string `json:"id"`
				} `json:"publicIPAddress"`
			} `json:"properties"`
		} `json:"ipConfigurations"`
	} `json:"properties"`
}

type publicIPAddressResource struct {
	ID         string            `json:"id"`
	Name       string            `json:"name"`
	Tags       map[string]string `json:"tags"`
	Properties struct {
		IPAddress   string `json:"ipAddress"`
		DNSSettings *struct {
			FQDN string `json:"fqdn"`
		} `json:"dnsSettings"`
	} `json:"properties"`
}

type taggedResource struct {
	ID   string            `json:"id"`
	Name string            `json:"name"`
	Tags map[string]string `json:"tags"`
}

type resourceSKU struct {
	Name         string   `json:"name"`
	ResourceType string   `json:"resourceType"`
	Locations    []string `json:"locations"`
	LocationInfo []struct {
		Location string   `json:"location"`
		Zones    []string `json:"zones"`
	} `json:"locationInfo"`
	Capabilities []struct {
		Name  string `json:"name"`
		Value string `json:"value"`
	} `json:"capabilities"`
	Restrictions []struct {
		Type            string `json:"type"`
		ReasonCode      string `json:"reasonCode"`
		RestrictionInfo *struct {
			Locations []string `json:"locations"`
		} `json:"restrictionInfo"`
	} `json:"restrictions"`
}

type armListResponse[T any] struct {
	Value    []T    `json:"value"`
	NextLink string `json:"nextLink"`
}

type armResponse struct {
	StatusCode int
	Headers    http.Header
	Body       []byte
}

type armErrorEnvelope struct {
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

type tokenResponse struct {
	AccessToken      string `json:"access_token"`
	ExpiresIn        int    `json:"expires_in"`
	Error            string `json:"error"`
	ErrorDescription string `json:"error_description"`
}

type armAsyncOperationStatus struct {
	Status string `json:"status"`
	Error  *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
	Properties *struct {
		ProvisioningState string `json:"provisioningState"`
	} `json:"properties"`
}

func NewClient(addition *Addition) (*Client, error) {
	if addition == nil {
		return nil, errors.New("azure configuration is missing")
	}
	addition.Normalize()
	active := addition.ActiveCredential()
	if active != nil {
		return newClient(active, defaultAuthorityURL, defaultManagementURL)
	}
	return nil, errors.New("azure credential is missing")
}

func NewClientFromCredential(credential *CredentialRecord) (*Client, error) {
	return newClient(credential, defaultAuthorityURL, defaultManagementURL)
}

func newClient(credential *CredentialRecord, authorityURL, managementURL string) (*Client, error) {
	if credential == nil {
		return nil, errors.New("azure credential is missing")
	}

	record := *credential
	record.TenantID = strings.TrimSpace(record.TenantID)
	record.ClientID = strings.TrimSpace(record.ClientID)
	record.ClientSecret = strings.TrimSpace(record.ClientSecret)
	record.SubscriptionID = strings.TrimSpace(record.SubscriptionID)
	record.DefaultLocation = normalizeLocation(record.DefaultLocation)

	if record.TenantID == "" || record.ClientID == "" || record.ClientSecret == "" || record.SubscriptionID == "" {
		return nil, errors.New("azure credential is incomplete")
	}

	authorityURL = strings.TrimRight(strings.TrimSpace(authorityURL), "/")
	if authorityURL == "" {
		authorityURL = defaultAuthorityURL
	}
	managementURL = strings.TrimRight(strings.TrimSpace(managementURL), "/")
	if managementURL == "" {
		managementURL = defaultManagementURL
	}

	return &Client{
		credential:    record,
		authorityURL:  authorityURL,
		managementURL: managementURL,
		httpClient:    outboundproxy.NewHTTPClient(25 * time.Second),
	}, nil
}

func (c *Client) GetSubscription(ctx context.Context) (*Subscription, error) {
	var subscription Subscription
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/subscriptions/%s", url.PathEscape(c.credential.SubscriptionID)), url.Values{
		"api-version": {subscriptionAPIVersion},
	}, nil, &subscription); err != nil {
		return nil, err
	}
	return &subscription, nil
}

func (c *Client) ListLocations(ctx context.Context) ([]Location, error) {
	var response armListResponse[Location]
	if err := c.do(ctx, http.MethodGet, fmt.Sprintf("/subscriptions/%s/locations", url.PathEscape(c.credential.SubscriptionID)), url.Values{
		"api-version": {subscriptionAPIVersion},
	}, nil, &response); err != nil {
		return nil, err
	}

	locations := response.Value
	sort.Slice(locations, func(i, j int) bool {
		left := strings.TrimSpace(firstNonEmpty(locations[i].RegionalDisplayName, locations[i].DisplayName, locations[i].Name))
		right := strings.TrimSpace(firstNonEmpty(locations[j].RegionalDisplayName, locations[j].DisplayName, locations[j].Name))
		return left < right
	})
	return locations, nil
}

func (c *Client) ListVirtualMachineSizes(ctx context.Context, location string) ([]VMSku, error) {
	location = normalizeLocation(location)
	if location == "" {
		location = firstNonEmpty(c.credential.DefaultLocation, DefaultLocation)
	}

	var resources []resourceSKU
	if err := listAll(ctx, c, fmt.Sprintf("/subscriptions/%s/providers/Microsoft.Compute/skus", url.PathEscape(c.credential.SubscriptionID)), url.Values{
		"api-version": {computeSKUAPIVersion},
		"$filter":     {fmt.Sprintf("location eq '%s'", location)},
	}, &resources); err != nil {
		return nil, err
	}

	sizes := make([]VMSku, 0, len(resources))
	seen := make(map[string]struct{}, len(resources))
	for _, resource := range resources {
		if !strings.EqualFold(strings.TrimSpace(resource.ResourceType), "virtualMachines") {
			continue
		}

		name := strings.TrimSpace(resource.Name)
		if name == "" {
			continue
		}
		if _, exists := seen[name]; exists {
			continue
		}
		if skuRestrictedForLocation(resource, location) {
			continue
		}
		seen[name] = struct{}{}

		sizes = append(sizes, VMSku{
			Name:      name,
			VCPUs:     skuCapabilityInt(resource.Capabilities, "vCPUs"),
			MemoryGB:  skuCapabilityFloat(resource.Capabilities, "MemoryGB"),
			Zones:     skuZonesForLocation(resource, location),
			MaxDataGB: skuCapabilityInt(resource.Capabilities, "MaxDataDiskCount"),
		})
	}

	sort.Slice(sizes, func(i, j int) bool {
		if sizes[i].VCPUs != sizes[j].VCPUs {
			return sizes[i].VCPUs < sizes[j].VCPUs
		}
		if sizes[i].MemoryGB != sizes[j].MemoryGB {
			return sizes[i].MemoryGB < sizes[j].MemoryGB
		}
		return sizes[i].Name < sizes[j].Name
	})

	return sizes, nil
}

func (c *Client) ListVirtualMachines(ctx context.Context) ([]Instance, error) {
	var resources []virtualMachineResource
	query := url.Values{
		"api-version": {computeAPIVersion},
		"$expand":     {"instanceView"},
	}
	if err := listAll(ctx, c, fmt.Sprintf("/subscriptions/%s/providers/Microsoft.Compute/virtualMachines", url.PathEscape(c.credential.SubscriptionID)), query, &resources); err != nil {
		if !canRetryVirtualMachineListWithoutExpand(err) {
			return nil, err
		}
		if err := listAll(ctx, c, fmt.Sprintf("/subscriptions/%s/providers/Microsoft.Compute/virtualMachines", url.PathEscape(c.credential.SubscriptionID)), url.Values{
			"api-version": {computeAPIVersion},
		}, &resources); err != nil {
			return nil, err
		}
	}

	instances := make([]Instance, len(resources))
	var wg sync.WaitGroup
	limiter := make(chan struct{}, 6)

	for index := range resources {
		wg.Add(1)
		go func(vmIndex int) {
			defer wg.Done()
			limiter <- struct{}{}
			defer func() {
				<-limiter
			}()

			nics, _ := c.resolveNetworkInterfaces(ctx, resources[vmIndex].Properties.NetworkProfile.NetworkInterfaces, false)
			instances[vmIndex] = buildInstance(resources[vmIndex], nics)
		}(index)
	}

	wg.Wait()

	sort.Slice(instances, func(i, j int) bool {
		if instances[i].ResourceGroup != instances[j].ResourceGroup {
			return instances[i].ResourceGroup < instances[j].ResourceGroup
		}
		return instances[i].Name < instances[j].Name
	})

	return instances, nil
}

func (c *Client) GetVirtualMachineDetail(ctx context.Context, resourceGroup, name string) (*InstanceDetail, error) {
	resourceGroup = strings.TrimSpace(resourceGroup)
	name = strings.TrimSpace(name)
	if resourceGroup == "" || name == "" {
		return nil, errors.New("azure vm resource group and name are required")
	}

	var resource virtualMachineResource
	if err := c.do(ctx, http.MethodGet, c.vmPath(resourceGroup, name), url.Values{
		"api-version": {computeAPIVersion},
	}, nil, &resource); err != nil {
		return nil, err
	}

	var runtime instanceView
	if err := c.do(ctx, http.MethodGet, c.vmPath(resourceGroup, name)+"/instanceView", url.Values{
		"api-version": {computeAPIVersion},
	}, nil, &runtime); err == nil {
		resource.Properties.InstanceView = &struct {
			Statuses []instanceStatus `json:"statuses"`
		}{
			Statuses: runtime.Statuses,
		}
	}

	nics, err := c.resolveNetworkInterfaces(ctx, resource.Properties.NetworkProfile.NetworkInterfaces, true)
	if err != nil {
		return nil, err
	}

	detail := &InstanceDetail{
		Instance:          buildInstance(resource, nics),
		VMID:              strings.TrimSpace(resource.Properties.VMID),
		Zones:             uniqueStrings(resource.Zones),
		LicenseType:       strings.TrimSpace(resource.Properties.LicenseType),
		NetworkInterfaces: nics,
		DataDisks:         make([]Disk, 0, len(resource.Properties.StorageProfile.DataDisks)),
	}

	if resource.Properties.StorageProfile.OSDisk != nil {
		detail.OSDisk = &Disk{
			ID:                 strings.TrimSpace(osManagedDiskID(resource.Properties.StorageProfile.OSDisk)),
			Name:               strings.TrimSpace(resource.Properties.StorageProfile.OSDisk.Name),
			SizeGB:             resource.Properties.StorageProfile.OSDisk.DiskSizeGB,
			CreateOption:       strings.TrimSpace(resource.Properties.StorageProfile.OSDisk.CreateOption),
			StorageAccountType: strings.TrimSpace(osManagedDiskType(resource.Properties.StorageProfile.OSDisk)),
		}
	}

	for _, disk := range resource.Properties.StorageProfile.DataDisks {
		detail.DataDisks = append(detail.DataDisks, Disk{
			ID:                 strings.TrimSpace(dataManagedDiskID(disk)),
			Name:               strings.TrimSpace(disk.Name),
			Lun:                disk.Lun,
			SizeGB:             disk.DiskSizeGB,
			CreateOption:       strings.TrimSpace(disk.CreateOption),
			StorageAccountType: strings.TrimSpace(dataManagedDiskType(disk)),
		})
	}

	return detail, nil
}

func (c *Client) CreateVirtualMachine(ctx context.Context, request CreateVirtualMachineRequest) (*InstanceDetail, error) {
	name := normalizeAzureResourceName(request.Name, 64, fmt.Sprintf("komari-vm-%d", time.Now().Unix()))
	resourceGroup := strings.TrimSpace(request.ResourceGroup)
	if resourceGroup == "" {
		resourceGroup = "komari-" + name
	}
	resourceGroup = normalizeAzureResourceName(resourceGroup, 90, "komari-"+name)
	location := normalizeLocation(request.Location)
	if location == "" {
		location = firstNonEmpty(c.credential.DefaultLocation, DefaultLocation)
	}

	size := strings.TrimSpace(request.Size)
	if size == "" {
		return nil, errors.New("azure vm size is required")
	}

	image := ImageReference{
		Publisher: strings.TrimSpace(request.Image.Publisher),
		Offer:     strings.TrimSpace(request.Image.Offer),
		SKU:       strings.TrimSpace(request.Image.SKU),
		Version:   strings.TrimSpace(request.Image.Version),
	}
	if image.Publisher == "" || image.Offer == "" || image.SKU == "" {
		return nil, errors.New("azure vm image publisher, offer, and sku are required")
	}
	if image.Version == "" {
		image.Version = "latest"
	}

	adminUsername := normalizeAzureLinuxUsername(request.AdminUsername)
	if adminUsername == "" {
		adminUsername = "azureuser"
	}
	adminPassword := strings.TrimSpace(request.AdminPassword)
	sshPublicKey := strings.TrimSpace(request.SSHPublicKey)
	if adminPassword == "" && sshPublicKey == "" {
		return nil, errors.New("azure vm requires either an admin password or an SSH public key")
	}

	tags := normalizeTags(request.Tags)
	if len(tags) == 0 {
		tags = map[string]string{}
	}
	if _, exists := tags["managed-by"]; !exists {
		tags["managed-by"] = "komari"
	}

	resourceBase := normalizeAzureResourceName(name, 48, "komari-vm")
	vnetName := resourceBase + "-vnet"
	subnetName := "default"
	nsgName := resourceBase + "-nsg"
	publicIPName := resourceBase + "-ip"
	nicName := resourceBase + "-nic"
	osDiskName := resourceBase + "-osdisk"

	if err := c.ensureResourceGroup(ctx, resourceGroup, location, tags); err != nil {
		return nil, err
	}
	if err := c.ensureVirtualNetwork(ctx, resourceGroup, location, vnetName, subnetName, request.AssignIPv6, tags); err != nil {
		c.cleanupVirtualMachineCreateFailure(ctx, resourceGroup, name, resourceBase, request.PublicIP, request.AssignIPv6)
		return nil, err
	}
	if err := c.ensureNetworkSecurityGroup(ctx, resourceGroup, location, nsgName, tags); err != nil {
		c.cleanupVirtualMachineCreateFailure(ctx, resourceGroup, name, resourceBase, request.PublicIP, request.AssignIPv6)
		return nil, err
	}

	publicIPv4ID := ""
	publicIPv6ID := ""
	if request.PublicIP {
		if err := c.ensurePublicIPAddress(ctx, resourceGroup, location, publicIPName, "IPv4", tags); err != nil {
			c.cleanupVirtualMachineCreateFailure(ctx, resourceGroup, name, resourceBase, request.PublicIP, request.AssignIPv6)
			return nil, err
		}
		publicIPv4ID = c.publicIPAddressPath(resourceGroup, publicIPName)
		if request.AssignIPv6 {
			publicIPv6Name := resourceBase + "-ip6"
			if err := c.ensurePublicIPAddress(ctx, resourceGroup, location, publicIPv6Name, "IPv6", tags); err != nil {
				c.cleanupVirtualMachineCreateFailure(ctx, resourceGroup, name, resourceBase, request.PublicIP, request.AssignIPv6)
				return nil, err
			}
			publicIPv6ID = c.publicIPAddressPath(resourceGroup, publicIPv6Name)
		}
	}

	if err := c.ensureNetworkInterface(
		ctx,
		resourceGroup,
		location,
		nicName,
		c.subnetPath(resourceGroup, vnetName, subnetName),
		c.networkSecurityGroupPath(resourceGroup, nsgName),
		publicIPv4ID,
		publicIPv6ID,
		request.AssignIPv6,
		tags,
	); err != nil {
		c.cleanupVirtualMachineCreateFailure(ctx, resourceGroup, name, resourceBase, request.PublicIP, request.AssignIPv6)
		return nil, err
	}
	if err := c.ensureVirtualMachine(ctx, resourceGroup, name, location, nicName, osDiskName, adminUsername, adminPassword, sshPublicKey, size, image, strings.TrimSpace(request.UserData), tags); err != nil {
		c.cleanupVirtualMachineCreateFailure(ctx, resourceGroup, name, resourceBase, request.PublicIP, request.AssignIPv6)
		return nil, err
	}

	detail, err := c.GetVirtualMachineDetail(ctx, resourceGroup, name)
	if err != nil {
		c.cleanupVirtualMachineCreateFailure(ctx, resourceGroup, name, resourceBase, request.PublicIP, request.AssignIPv6)
		return nil, err
	}
	return detail, nil
}

func (c *Client) StartVirtualMachine(ctx context.Context, resourceGroup, name string) error {
	return c.do(ctx, http.MethodPost, c.vmPath(resourceGroup, name)+"/start", url.Values{
		"api-version": {computeAPIVersion},
	}, map[string]any{}, nil)
}

func (c *Client) RestartVirtualMachine(ctx context.Context, resourceGroup, name string) error {
	return c.do(ctx, http.MethodPost, c.vmPath(resourceGroup, name)+"/restart", url.Values{
		"api-version": {computeAPIVersion},
	}, map[string]any{}, nil)
}

func (c *Client) DeallocateVirtualMachine(ctx context.Context, resourceGroup, name string) error {
	return c.do(ctx, http.MethodPost, c.vmPath(resourceGroup, name)+"/deallocate", url.Values{
		"api-version": {computeAPIVersion},
	}, map[string]any{}, nil)
}

func (c *Client) DeleteVirtualMachine(ctx context.Context, resourceGroup, name string) (*DeleteVirtualMachineResult, error) {
	resourceGroup = strings.TrimSpace(resourceGroup)
	name = strings.TrimSpace(name)
	if resourceGroup == "" || name == "" {
		return nil, errors.New("azure vm resource group and name are required")
	}

	result := &DeleteVirtualMachineResult{}
	var vmResource virtualMachineResource
	if err := c.do(ctx, http.MethodGet, c.vmPath(resourceGroup, name), url.Values{
		"api-version": {computeAPIVersion},
	}, nil, &vmResource); err != nil {
		return result, err
	}

	cleanupResources := c.collectVirtualMachineCleanupResources(ctx, resourceGroup, vmResource, result)
	if err := c.doAsync(ctx, http.MethodDelete, c.vmPath(resourceGroup, name), url.Values{
		"api-version": {computeAPIVersion},
	}, nil); err != nil {
		return result, err
	}
	result.addDeletedResource(firstNonEmpty(vmResource.ID, c.vmPath(resourceGroup, name)))

	for _, resource := range cleanupResources {
		if err := c.deleteAssociatedResource(ctx, resource); err != nil {
			result.addCleanupError(fmt.Sprintf("delete %s %s: %v", resource.Kind, firstNonEmpty(resource.ID, resource.Name), err))
			continue
		}
		result.addDeletedResource(resource.ID)
	}

	return result, nil
}

func (c *Client) ReplaceVirtualMachinePublicIPv4(ctx context.Context, resourceGroup, name string) (*PublicIPReplacementResult, error) {
	resourceGroup = strings.TrimSpace(resourceGroup)
	name = strings.TrimSpace(name)
	if resourceGroup == "" || name == "" {
		return nil, errors.New("azure vm resource group and name are required")
	}

	var vmResource virtualMachineResource
	if err := c.do(ctx, http.MethodGet, c.vmPath(resourceGroup, name), url.Values{
		"api-version": {computeAPIVersion},
	}, nil, &vmResource); err != nil {
		return nil, err
	}

	primaryNICID := ""
	for _, nicRef := range vmResource.Properties.NetworkProfile.NetworkInterfaces {
		if nicRef.Properties.Primary {
			primaryNICID = strings.TrimSpace(nicRef.ID)
			break
		}
	}
	if primaryNICID == "" && len(vmResource.Properties.NetworkProfile.NetworkInterfaces) > 0 {
		primaryNICID = strings.TrimSpace(vmResource.Properties.NetworkProfile.NetworkInterfaces[0].ID)
	}
	if primaryNICID == "" {
		return nil, errors.New("azure vm has no network interface attached")
	}

	nicPath, err := toManagementPath(primaryNICID)
	if err != nil {
		return nil, err
	}

	var nicResource map[string]any
	if err := c.do(ctx, http.MethodGet, nicPath, url.Values{
		"api-version": {networkAPIVersion},
	}, nil, &nicResource); err != nil {
		return nil, err
	}

	properties := mapFromAny(nicResource["properties"])
	if properties == nil {
		return nil, errors.New("azure network interface properties are missing")
	}
	ipConfigurations := sliceFromAny(properties["ipConfigurations"])
	if len(ipConfigurations) == 0 {
		return nil, errors.New("azure network interface has no ip configurations")
	}

	targetIndex := 0
	for index, rawConfig := range ipConfigurations {
		configMap := mapFromAny(rawConfig)
		configProperties := mapFromAny(configMap["properties"])
		if configProperties == nil {
			continue
		}
		if boolFromAny(configProperties["primary"]) {
			targetIndex = index
			break
		}
	}

	targetConfig := mapFromAny(ipConfigurations[targetIndex])
	if targetConfig == nil {
		return nil, errors.New("azure network interface ip configuration is invalid")
	}
	targetConfigProperties := mapFromAny(targetConfig["properties"])
	if targetConfigProperties == nil {
		return nil, errors.New("azure network interface ip configuration properties are missing")
	}

	oldPublicIPID := ""
	oldPublicIP := ""
	if oldPublicIPRaw := mapFromAny(targetConfigProperties["publicIPAddress"]); oldPublicIPRaw != nil {
		oldPublicIPID = strings.TrimSpace(stringFromAny(oldPublicIPRaw["id"]))
		if oldPublicIPID != "" {
			oldPublicIP, _ = c.getPublicIPAddress(ctx, oldPublicIPID)
		}
	}

	location := normalizeLocation(firstNonEmpty(stringFromAny(nicResource["location"]), vmResource.Location, c.credential.DefaultLocation, DefaultLocation))
	if location == "" {
		location = DefaultLocation
	}
	tags := normalizeTags(anyMapString(nicResource["tags"]))
	newPublicIPName := normalizeAzureResourceName(fmt.Sprintf("%s-rip-%d", name, time.Now().Unix()), 80, fmt.Sprintf("komari-%d", time.Now().Unix()))
	if err := c.ensurePublicIPAddress(ctx, resourceGroup, location, newPublicIPName, "IPv4", tags); err != nil {
		return nil, err
	}
	newPublicIPID := c.publicIPAddressPath(resourceGroup, newPublicIPName)

	targetConfigProperties["publicIPAddress"] = map[string]any{
		"id": newPublicIPID,
	}
	targetConfig["properties"] = targetConfigProperties
	ipConfigurations[targetIndex] = targetConfig
	properties["ipConfigurations"] = ipConfigurations

	payload := map[string]any{
		"location":   location,
		"properties": properties,
	}
	if tagsRaw := nicResource["tags"]; tagsRaw != nil {
		payload["tags"] = tagsRaw
	}

	if err := c.doAsync(ctx, http.MethodPut, nicPath, url.Values{
		"api-version": {networkAPIVersion},
	}, payload); err != nil {
		return nil, err
	}

	if oldPublicIPID != "" {
		if oldPath, pathErr := toManagementPath(oldPublicIPID); pathErr == nil {
			_ = c.doAsync(ctx, http.MethodDelete, oldPath, url.Values{
				"api-version": {networkAPIVersion},
			}, nil)
		}
	}

	newPublicIP, _ := c.getPublicIPAddress(ctx, newPublicIPID)

	return &PublicIPReplacementResult{
		OldPublicIPID: oldPublicIPID,
		OldPublicIP:   strings.TrimSpace(oldPublicIP),
		NewPublicIPID: newPublicIPID,
		NewPublicIP:   strings.TrimSpace(newPublicIP),
	}, nil
}

type virtualMachineCleanupResource struct {
	ID         string
	Name       string
	Kind       string
	APIVersion string
}

func (r *DeleteVirtualMachineResult) addDeletedResource(resourceID string) {
	if r == nil {
		return
	}
	resourceID = strings.TrimSpace(resourceID)
	if resourceID == "" {
		return
	}
	for _, existing := range r.DeletedResourceIDs {
		if strings.EqualFold(existing, resourceID) {
			return
		}
	}
	r.DeletedResourceIDs = append(r.DeletedResourceIDs, resourceID)
}

func (r *DeleteVirtualMachineResult) addCleanupError(message string) {
	if r == nil {
		return
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return
	}
	r.CleanupErrors = append(r.CleanupErrors, message)
}

func (c *Client) collectVirtualMachineCleanupResources(ctx context.Context, resourceGroup string, vm virtualMachineResource, result *DeleteVirtualMachineResult) []virtualMachineCleanupResource {
	vmManaged := isKomariManagedTags(vm.Tags)
	var nicResources []virtualMachineCleanupResource
	var publicIPResources []virtualMachineCleanupResource
	var diskResources []virtualMachineCleanupResource
	var securityGroupResources []virtualMachineCleanupResource
	var vnetResources []virtualMachineCleanupResource

	if vmManaged && vm.Properties.StorageProfile.OSDisk != nil && vm.Properties.StorageProfile.OSDisk.ManagedDisk != nil {
		diskID := strings.TrimSpace(vm.Properties.StorageProfile.OSDisk.ManagedDisk.ID)
		if diskID != "" {
			diskResources = appendCleanupResource(diskResources, virtualMachineCleanupResource{
				ID:         diskID,
				Name:       vm.Properties.StorageProfile.OSDisk.Name,
				Kind:       "managed disk",
				APIVersion: computeAPIVersion,
			})
		}
	}

	for _, nicRef := range vm.Properties.NetworkProfile.NetworkInterfaces {
		nicID := strings.TrimSpace(nicRef.ID)
		if nicID == "" {
			continue
		}
		nicPath, err := toManagementPath(nicID)
		if err != nil {
			result.addCleanupError(fmt.Sprintf("inspect network interface %s: %v", nicID, err))
			continue
		}

		var nic networkInterfaceResource
		if err := c.do(ctx, http.MethodGet, nicPath, url.Values{
			"api-version": {networkAPIVersion},
		}, nil, &nic); err != nil {
			if !isAzureNotFoundError(err) {
				result.addCleanupError(fmt.Sprintf("inspect network interface %s: %v", nicID, err))
			}
			continue
		}

		nicManaged := isKomariManagedTags(nic.Tags)
		if nicManaged {
			nicResources = appendCleanupResource(nicResources, virtualMachineCleanupResource{
				ID:         firstNonEmpty(nic.ID, nicID),
				Name:       nic.Name,
				Kind:       "network interface",
				APIVersion: networkAPIVersion,
			})
		}

		if !vmManaged && !nicManaged {
			continue
		}

		if nic.Properties.NetworkSecurityGroup != nil {
			if resource, ok := c.managedTaggedCleanupResource(ctx, nic.Properties.NetworkSecurityGroup.ID, "network security group", networkAPIVersion, result); ok {
				securityGroupResources = appendCleanupResource(securityGroupResources, resource)
			}
		}

		for _, config := range nic.Properties.IPConfigurations {
			if config.Properties.PublicIPAddress != nil {
				if resource, ok := c.managedTaggedCleanupResource(ctx, config.Properties.PublicIPAddress.ID, "public IP address", networkAPIVersion, result); ok {
					publicIPResources = appendCleanupResource(publicIPResources, resource)
				}
			}
			if config.Properties.Subnet != nil {
				if vnetID := parentAzureResourceID(config.Properties.Subnet.ID, "/subnets/"); vnetID != "" {
					if resource, ok := c.managedTaggedCleanupResource(ctx, vnetID, "virtual network", networkAPIVersion, result); ok {
						vnetResources = appendCleanupResource(vnetResources, resource)
					}
				}
			}
		}
	}

	resources := make([]virtualMachineCleanupResource, 0, len(nicResources)+len(publicIPResources)+len(diskResources)+len(securityGroupResources)+len(vnetResources))
	resources = append(resources, nicResources...)
	resources = append(resources, publicIPResources...)
	resources = append(resources, diskResources...)
	resources = append(resources, securityGroupResources...)
	resources = append(resources, vnetResources...)
	return resources
}

func (c *Client) managedTaggedCleanupResource(ctx context.Context, resourceID, kind, apiVersion string, result *DeleteVirtualMachineResult) (virtualMachineCleanupResource, bool) {
	resourceID = strings.TrimSpace(resourceID)
	if resourceID == "" {
		return virtualMachineCleanupResource{}, false
	}
	path, err := toManagementPath(resourceID)
	if err != nil {
		result.addCleanupError(fmt.Sprintf("inspect %s %s: %v", kind, resourceID, err))
		return virtualMachineCleanupResource{}, false
	}

	var resource taggedResource
	if err := c.do(ctx, http.MethodGet, path, url.Values{
		"api-version": {apiVersion},
	}, nil, &resource); err != nil {
		if !isAzureNotFoundError(err) {
			result.addCleanupError(fmt.Sprintf("inspect %s %s: %v", kind, resourceID, err))
		}
		return virtualMachineCleanupResource{}, false
	}
	if !isKomariManagedTags(resource.Tags) {
		return virtualMachineCleanupResource{}, false
	}
	return virtualMachineCleanupResource{
		ID:         firstNonEmpty(resource.ID, resourceID),
		Name:       resource.Name,
		Kind:       kind,
		APIVersion: apiVersion,
	}, true
}

func appendCleanupResource(resources []virtualMachineCleanupResource, resource virtualMachineCleanupResource) []virtualMachineCleanupResource {
	resource.ID = strings.TrimSpace(resource.ID)
	if resource.ID == "" {
		return resources
	}
	for _, existing := range resources {
		if strings.EqualFold(existing.ID, resource.ID) {
			return resources
		}
	}
	return append(resources, resource)
}

func (c *Client) deleteAssociatedResource(ctx context.Context, resource virtualMachineCleanupResource) error {
	if strings.TrimSpace(resource.ID) == "" {
		return nil
	}
	path, err := toManagementPath(resource.ID)
	if err != nil {
		return err
	}
	if err := c.doAsync(ctx, http.MethodDelete, path, url.Values{
		"api-version": {resource.APIVersion},
	}, nil); err != nil {
		if isAzureNotFoundError(err) {
			return nil
		}
		return err
	}
	return nil
}

func (c *Client) cleanupVirtualMachineCreateFailure(ctx context.Context, resourceGroup, name, resourceBase string, publicIP, assignIPv6 bool) {
	_ = c.doAsync(ctx, http.MethodDelete, c.vmPath(resourceGroup, name), url.Values{
		"api-version": {computeAPIVersion},
	}, nil)

	result := &DeleteVirtualMachineResult{}
	resources := make([]virtualMachineCleanupResource, 0, 5)
	appendKnownTagged := func(resourceID, kind string) {
		if resource, ok := c.managedTaggedCleanupResource(ctx, resourceID, kind, networkAPIVersion, result); ok {
			resources = appendCleanupResource(resources, resource)
		}
	}

	appendKnownTagged(c.networkInterfacePath(resourceGroup, resourceBase+"-nic"), "network interface")
	if publicIP {
		appendKnownTagged(c.publicIPAddressPath(resourceGroup, resourceBase+"-ip"), "public IP address")
		if assignIPv6 {
			appendKnownTagged(c.publicIPAddressPath(resourceGroup, resourceBase+"-ip6"), "public IP address")
		}
	}
	appendKnownTagged(c.networkSecurityGroupPath(resourceGroup, resourceBase+"-nsg"), "network security group")
	appendKnownTagged(c.virtualNetworkPath(resourceGroup, resourceBase+"-vnet"), "virtual network")

	for _, resource := range resources {
		_ = c.deleteAssociatedResource(ctx, resource)
	}
}

func EncodeInstanceID(resourceGroup, name string) string {
	raw := strings.TrimSpace(resourceGroup) + "\n" + strings.TrimSpace(name)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func DecodeInstanceID(instanceID string) (string, string, error) {
	instanceID = strings.TrimSpace(instanceID)
	if instanceID == "" {
		return "", "", errors.New("azure vm instance id is empty")
	}

	decoded, err := base64.RawURLEncoding.DecodeString(instanceID)
	if err != nil {
		return "", "", fmt.Errorf("invalid azure vm instance id: %w", err)
	}

	parts := strings.SplitN(string(decoded), "\n", 2)
	if len(parts) != 2 {
		return "", "", errors.New("invalid azure vm instance id")
	}

	resourceGroup := strings.TrimSpace(parts[0])
	name := strings.TrimSpace(parts[1])
	if resourceGroup == "" || name == "" {
		return "", "", errors.New("invalid azure vm instance id")
	}
	return resourceGroup, name, nil
}

func (c *Client) resolveNetworkInterfaces(ctx context.Context, refs []struct {
	ID         string `json:"id"`
	Properties struct {
		Primary bool `json:"primary"`
	} `json:"properties"`
}, includeAll bool) ([]NetworkInterface, error) {
	if len(refs) == 0 {
		return []NetworkInterface{}, nil
	}

	selected := refs
	if !includeAll {
		for _, item := range refs {
			if item.Properties.Primary {
				selected = []struct {
					ID         string `json:"id"`
					Properties struct {
						Primary bool `json:"primary"`
					} `json:"properties"`
				}{item}
				break
			}
		}
		if len(selected) > 1 {
			selected = selected[:1]
		}
	}

	interfaces := make([]NetworkInterface, 0, len(selected))
	for _, ref := range selected {
		nic, err := c.getNetworkInterface(ctx, ref.ID)
		if err != nil {
			return nil, err
		}
		nic.Primary = nic.Primary || ref.Properties.Primary
		interfaces = append(interfaces, nic)
	}
	return interfaces, nil
}

func (c *Client) getNetworkInterface(ctx context.Context, resourceID string) (NetworkInterface, error) {
	path, err := toManagementPath(resourceID)
	if err != nil {
		return NetworkInterface{}, err
	}

	var resource networkInterfaceResource
	if err := c.do(ctx, http.MethodGet, path, url.Values{
		"api-version": {networkAPIVersion},
	}, nil, &resource); err != nil {
		return NetworkInterface{}, err
	}

	result := NetworkInterface{
		ID:         strings.TrimSpace(resource.ID),
		Name:       strings.TrimSpace(resource.Name),
		PrivateIPs: make([]string, 0),
		PublicIPs:  make([]string, 0),
		SubnetIDs:  make([]string, 0),
	}
	if resource.Properties.NetworkSecurityGroup != nil {
		result.NetworkSecurityGroupID = strings.TrimSpace(resource.Properties.NetworkSecurityGroup.ID)
	}

	for _, config := range resource.Properties.IPConfigurations {
		if config.Properties.Primary {
			result.Primary = true
		}
		if privateIP := strings.TrimSpace(config.Properties.PrivateIPAddress); privateIP != "" {
			result.PrivateIPs = append(result.PrivateIPs, privateIP)
		}
		if config.Properties.Subnet != nil {
			if subnetID := strings.TrimSpace(config.Properties.Subnet.ID); subnetID != "" {
				result.SubnetIDs = append(result.SubnetIDs, subnetID)
			}
		}
		if config.Properties.PublicIPAddress != nil && strings.TrimSpace(config.Properties.PublicIPAddress.ID) != "" {
			address, err := c.getPublicIPAddress(ctx, config.Properties.PublicIPAddress.ID)
			if err == nil && strings.TrimSpace(address) != "" {
				result.PublicIPs = append(result.PublicIPs, address)
			}
		}
	}

	result.PrivateIPs = uniqueStrings(result.PrivateIPs)
	result.PublicIPs = uniqueStrings(result.PublicIPs)
	result.SubnetIDs = uniqueStrings(result.SubnetIDs)
	return result, nil
}

func (c *Client) getPublicIPAddress(ctx context.Context, resourceID string) (string, error) {
	path, err := toManagementPath(resourceID)
	if err != nil {
		return "", err
	}

	var resource publicIPAddressResource
	if err := c.do(ctx, http.MethodGet, path, url.Values{
		"api-version": {networkAPIVersion},
	}, nil, &resource); err != nil {
		return "", err
	}

	if address := strings.TrimSpace(resource.Properties.IPAddress); address != "" {
		return address, nil
	}
	if resource.Properties.DNSSettings != nil {
		return strings.TrimSpace(resource.Properties.DNSSettings.FQDN), nil
	}
	return "", nil
}

func listAll[T any](ctx context.Context, client *Client, path string, query url.Values, target *[]T) error {
	if target == nil {
		return errors.New("azure list target is required")
	}

	nextURL, err := client.buildURL(path, query)
	if err != nil {
		return err
	}

	items := make([]T, 0)
	for nextURL != "" {
		var response armListResponse[T]
		if err := client.doURL(ctx, http.MethodGet, nextURL, nil, &response); err != nil {
			return err
		}
		items = append(items, response.Value...)
		nextURL = strings.TrimSpace(response.NextLink)
	}

	*target = items
	return nil
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, requestBody any, responseBody any) error {
	requestURL, err := c.buildURL(path, query)
	if err != nil {
		return err
	}
	return c.doURL(ctx, method, requestURL, requestBody, responseBody)
}

func (c *Client) doAsync(ctx context.Context, method, path string, query url.Values, requestBody any) error {
	requestURL, err := c.buildURL(path, query)
	if err != nil {
		return err
	}
	return c.doAsyncURL(ctx, method, requestURL, requestBody)
}

func (c *Client) doURL(ctx context.Context, method, requestURL string, requestBody any, responseBody any) error {
	response, err := c.doRequest(ctx, method, requestURL, requestBody)
	if err != nil {
		return err
	}
	if response.StatusCode >= 400 {
		return parseAzureAPIError(response.StatusCode, response.Body)
	}
	if responseBody == nil || len(bytes.TrimSpace(response.Body)) == 0 {
		return nil
	}
	if err := json.Unmarshal(response.Body, responseBody); err != nil {
		return err
	}
	return nil
}

func (c *Client) doAsyncURL(ctx context.Context, method, requestURL string, requestBody any) error {
	response, err := c.doRequest(ctx, method, requestURL, requestBody)
	if err != nil {
		return err
	}
	if response.StatusCode >= 400 {
		return parseAzureAPIError(response.StatusCode, response.Body)
	}
	if !requiresAsyncPolling(response.StatusCode, response.Headers, response.Body) {
		return nil
	}
	return c.pollAsyncOperation(ctx, requestURL, response)
}

func (c *Client) doRequest(ctx context.Context, method, requestURL string, requestBody any) (*armResponse, error) {
	token, err := c.accessTokenFor(ctx)
	if err != nil {
		return nil, err
	}

	var bodyReader io.Reader
	if requestBody != nil {
		payload, marshalErr := json.Marshal(requestBody)
		if marshalErr != nil {
			return nil, marshalErr
		}
		bodyReader = bytes.NewReader(payload)
	}

	request, err := http.NewRequestWithContext(ctx, method, requestURL, bodyReader)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("User-Agent", "komari-cloud-azure")
	if requestBody != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	payload, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	return &armResponse{
		StatusCode: response.StatusCode,
		Headers:    response.Header.Clone(),
		Body:       payload,
	}, nil
}

func (c *Client) pollAsyncOperation(ctx context.Context, resourceURL string, response *armResponse) error {
	if response == nil {
		return errors.New("azure async response is missing")
	}

	delay := retryAfterDelay(response.Headers.Get("Retry-After"))
	if delay <= 0 {
		delay = 3 * time.Second
	}

	pollURL := strings.TrimSpace(response.Headers.Get("Azure-AsyncOperation"))
	if pollURL == "" {
		pollURL = strings.TrimSpace(response.Headers.Get("Location"))
	}

	if pollURL != "" {
		for {
			if err := waitForAzureRetry(ctx, delay); err != nil {
				return err
			}

			pollResponse, err := c.doRequest(ctx, http.MethodGet, pollURL, nil)
			if err != nil {
				return err
			}
			if pollResponse.StatusCode >= 400 {
				return parseAzureAPIError(pollResponse.StatusCode, pollResponse.Body)
			}

			done, nextDelay, err := interpretAzureAsyncResponse(pollResponse)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
			if nextDelay > 0 {
				delay = nextDelay
			}
		}
	}

	return c.pollResourceProvisioningState(ctx, resourceURL, delay)
}

func (c *Client) pollResourceProvisioningState(ctx context.Context, resourceURL string, delay time.Duration) error {
	for {
		if err := waitForAzureRetry(ctx, delay); err != nil {
			return err
		}

		response, err := c.doRequest(ctx, http.MethodGet, resourceURL, nil)
		if err != nil {
			return err
		}
		if response.StatusCode == http.StatusNotFound {
			return nil
		}
		if response.StatusCode >= 400 {
			return parseAzureAPIError(response.StatusCode, response.Body)
		}

		provisioningState := strings.ToLower(strings.TrimSpace(extractProvisioningState(response.Body)))
		switch provisioningState {
		case "", "succeeded":
			return nil
		case "failed", "canceled":
			return parseAzureProvisioningError(response.Body, provisioningState)
		default:
			if nextDelay := retryAfterDelay(response.Headers.Get("Retry-After")); nextDelay > 0 {
				delay = nextDelay
			}
		}
	}
}

func (c *Client) accessTokenFor(ctx context.Context) (string, error) {
	c.mu.Lock()
	if strings.TrimSpace(c.accessToken) != "" && time.Until(c.tokenExpiry) > time.Minute {
		token := c.accessToken
		c.mu.Unlock()
		return token, nil
	}
	c.mu.Unlock()

	form := url.Values{
		"client_id":     {c.credential.ClientID},
		"client_secret": {c.credential.ClientSecret},
		"scope":         {"https://management.azure.com/.default"},
		"grant_type":    {"client_credentials"},
	}

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		fmt.Sprintf("%s/%s/oauth2/v2.0/token", c.authorityURL, url.PathEscape(c.credential.TenantID)),
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return "", err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "komari-cloud-azure")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	payload, err := io.ReadAll(response.Body)
	if err != nil {
		return "", err
	}

	var token tokenResponse
	if len(bytes.TrimSpace(payload)) > 0 {
		_ = json.Unmarshal(payload, &token)
	}

	if response.StatusCode >= 400 {
		message := strings.TrimSpace(token.ErrorDescription)
		if message == "" {
			message = strings.TrimSpace(token.Error)
		}
		return "", &APIError{
			StatusCode: response.StatusCode,
			Code:       strings.TrimSpace(token.Error),
			Message:    message,
		}
	}
	if strings.TrimSpace(token.AccessToken) == "" {
		return "", errors.New("azure access token response was empty")
	}

	expiry := time.Now().Add(time.Duration(token.ExpiresIn) * time.Second)
	c.mu.Lock()
	c.accessToken = token.AccessToken
	c.tokenExpiry = expiry
	c.mu.Unlock()

	return token.AccessToken, nil
}

func (c *Client) buildURL(path string, query url.Values) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", errors.New("azure request path is empty")
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	base, err := url.Parse(c.managementURL)
	if err != nil {
		return "", err
	}
	base.Path = path
	if query != nil {
		base.RawQuery = query.Encode()
	}
	return base.String(), nil
}

func (c *Client) vmPath(resourceGroup, name string) string {
	return fmt.Sprintf(
		"/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/virtualMachines/%s",
		url.PathEscape(strings.TrimSpace(c.credential.SubscriptionID)),
		url.PathEscape(strings.TrimSpace(resourceGroup)),
		url.PathEscape(strings.TrimSpace(name)),
	)
}

func (c *Client) resourceGroupPath(resourceGroup string) string {
	return fmt.Sprintf(
		"/subscriptions/%s/resourcegroups/%s",
		url.PathEscape(strings.TrimSpace(c.credential.SubscriptionID)),
		url.PathEscape(strings.TrimSpace(resourceGroup)),
	)
}

func (c *Client) virtualNetworkPath(resourceGroup, name string) string {
	return fmt.Sprintf(
		"/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/virtualNetworks/%s",
		url.PathEscape(strings.TrimSpace(c.credential.SubscriptionID)),
		url.PathEscape(strings.TrimSpace(resourceGroup)),
		url.PathEscape(strings.TrimSpace(name)),
	)
}

func (c *Client) subnetPath(resourceGroup, vnetName, subnetName string) string {
	return fmt.Sprintf(
		"/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/virtualNetworks/%s/subnets/%s",
		url.PathEscape(strings.TrimSpace(c.credential.SubscriptionID)),
		url.PathEscape(strings.TrimSpace(resourceGroup)),
		url.PathEscape(strings.TrimSpace(vnetName)),
		url.PathEscape(strings.TrimSpace(subnetName)),
	)
}

func (c *Client) networkSecurityGroupPath(resourceGroup, name string) string {
	return fmt.Sprintf(
		"/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/networkSecurityGroups/%s",
		url.PathEscape(strings.TrimSpace(c.credential.SubscriptionID)),
		url.PathEscape(strings.TrimSpace(resourceGroup)),
		url.PathEscape(strings.TrimSpace(name)),
	)
}

func (c *Client) publicIPAddressPath(resourceGroup, name string) string {
	return fmt.Sprintf(
		"/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/publicIPAddresses/%s",
		url.PathEscape(strings.TrimSpace(c.credential.SubscriptionID)),
		url.PathEscape(strings.TrimSpace(resourceGroup)),
		url.PathEscape(strings.TrimSpace(name)),
	)
}

func (c *Client) networkInterfacePath(resourceGroup, name string) string {
	return fmt.Sprintf(
		"/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Network/networkInterfaces/%s",
		url.PathEscape(strings.TrimSpace(c.credential.SubscriptionID)),
		url.PathEscape(strings.TrimSpace(resourceGroup)),
		url.PathEscape(strings.TrimSpace(name)),
	)
}

func (c *Client) ensureResourceGroup(ctx context.Context, resourceGroup, location string, tags map[string]string) error {
	return c.doAsync(ctx, http.MethodPut, c.resourceGroupPath(resourceGroup), url.Values{
		"api-version": {resourceAPIVersion},
	}, map[string]any{
		"location": location,
		"tags":     tags,
	})
}

func buildVirtualNetworkProperties(subnetName string, assignIPv6 bool) map[string]any {
	addressPrefixes := []string{azureVNetIPv4CIDR}
	subnetProperties := map[string]any{
		"addressPrefix": azureSubnetIPv4CIDR,
	}
	if assignIPv6 {
		addressPrefixes = append(addressPrefixes, azureVNetIPv6CIDR)
		delete(subnetProperties, "addressPrefix")
		subnetProperties["addressPrefixes"] = []string{azureSubnetIPv4CIDR, azureSubnetIPv6CIDR}
	}

	return map[string]any{
		"addressSpace": map[string]any{
			"addressPrefixes": addressPrefixes,
		},
		"subnets": []map[string]any{
			{
				"name":       subnetName,
				"properties": subnetProperties,
			},
		},
	}
}

func (c *Client) ensureVirtualNetwork(ctx context.Context, resourceGroup, location, vnetName, subnetName string, assignIPv6 bool, tags map[string]string) error {
	return c.doAsync(ctx, http.MethodPut, c.virtualNetworkPath(resourceGroup, vnetName), url.Values{
		"api-version": {networkAPIVersion},
	}, map[string]any{
		"location":   location,
		"tags":       tags,
		"properties": buildVirtualNetworkProperties(subnetName, assignIPv6),
	})
}

func (c *Client) ensureNetworkSecurityGroup(ctx context.Context, resourceGroup, location, name string, tags map[string]string) error {
	return c.doAsync(ctx, http.MethodPut, c.networkSecurityGroupPath(resourceGroup, name), url.Values{
		"api-version": {networkAPIVersion},
	}, map[string]any{
		"location": location,
		"tags":     tags,
		"properties": map[string]any{
			"securityRules": []map[string]any{
				{
					"name": "allow-all-inbound",
					"properties": map[string]any{
						"priority":                 100,
						"protocol":                 "*",
						"access":                   "Allow",
						"direction":                "Inbound",
						"sourceAddressPrefix":      "*",
						"sourcePortRange":          "*",
						"destinationAddressPrefix": "*",
						"destinationPortRange":     "*",
					},
				},
			},
		},
	})
}

func (c *Client) ensurePublicIPAddress(ctx context.Context, resourceGroup, location, name, version string, tags map[string]string) error {
	version = strings.TrimSpace(version)
	if version == "" {
		version = "IPv4"
	}

	return c.doAsync(ctx, http.MethodPut, c.publicIPAddressPath(resourceGroup, name), url.Values{
		"api-version": {networkAPIVersion},
	}, map[string]any{
		"location": location,
		"tags":     tags,
		"sku": map[string]any{
			"name": "Standard",
		},
		"properties": map[string]any{
			"publicIPAllocationMethod": "Static",
			"publicIPAddressVersion":   version,
		},
	})
}

func buildNetworkInterfaceIPConfigurations(subnetID, publicIPv4ID, publicIPv6ID string, assignIPv6 bool) []map[string]any {
	ipv4Properties := map[string]any{
		"primary":                   true,
		"privateIPAddressVersion":   "IPv4",
		"privateIPAllocationMethod": "Dynamic",
		"subnet": map[string]any{
			"id": subnetID,
		},
	}
	if strings.TrimSpace(publicIPv4ID) != "" {
		ipv4Properties["publicIPAddress"] = map[string]any{
			"id": publicIPv4ID,
		}
	}

	configurations := []map[string]any{
		{
			"name":       "ipconfig-ipv4",
			"properties": ipv4Properties,
		},
	}
	if assignIPv6 {
		ipv6Properties := map[string]any{
			"privateIPAddressVersion":   "IPv6",
			"privateIPAllocationMethod": "Dynamic",
			"subnet": map[string]any{
				"id": subnetID,
			},
		}
		if strings.TrimSpace(publicIPv6ID) != "" {
			ipv6Properties["publicIPAddress"] = map[string]any{
				"id": publicIPv6ID,
			}
		}
		configurations = append(configurations, map[string]any{
			"name":       "ipconfig-ipv6",
			"properties": ipv6Properties,
		})
	}
	return configurations
}

func (c *Client) ensureNetworkInterface(ctx context.Context, resourceGroup, location, nicName, subnetID, networkSecurityGroupID, publicIPv4ID, publicIPv6ID string, assignIPv6 bool, tags map[string]string) error {
	properties := map[string]any{
		"ipConfigurations": buildNetworkInterfaceIPConfigurations(subnetID, publicIPv4ID, publicIPv6ID, assignIPv6),
	}
	if strings.TrimSpace(networkSecurityGroupID) != "" {
		properties["networkSecurityGroup"] = map[string]any{
			"id": networkSecurityGroupID,
		}
	}

	return c.doAsync(ctx, http.MethodPut, c.networkInterfacePath(resourceGroup, nicName), url.Values{
		"api-version": {networkAPIVersion},
	}, map[string]any{
		"location":   location,
		"tags":       tags,
		"properties": properties,
	})
}

func (c *Client) ensureVirtualMachine(ctx context.Context, resourceGroup, name, location, nicName, osDiskName, adminUsername, adminPassword, sshPublicKey, size string, image ImageReference, userData string, tags map[string]string) error {
	linuxConfiguration := map[string]any{
		"disablePasswordAuthentication": strings.TrimSpace(adminPassword) == "",
		"provisionVMAgent":              true,
	}
	if strings.TrimSpace(sshPublicKey) != "" {
		linuxConfiguration["ssh"] = map[string]any{
			"publicKeys": []map[string]any{
				{
					"path":    fmt.Sprintf("/home/%s/.ssh/authorized_keys", adminUsername),
					"keyData": strings.TrimSpace(sshPublicKey),
				},
			},
		}
	}

	osProfile := map[string]any{
		"computerName":       normalizeAzureResourceName(name, 64, "komari-vm"),
		"adminUsername":      adminUsername,
		"linuxConfiguration": linuxConfiguration,
	}
	if strings.TrimSpace(adminPassword) != "" {
		osProfile["adminPassword"] = strings.TrimSpace(adminPassword)
	}
	if userData = strings.TrimSpace(userData); userData != "" {
		osProfile["customData"] = base64.StdEncoding.EncodeToString([]byte(userData))
	}

	return c.doAsync(ctx, http.MethodPut, c.vmPath(resourceGroup, name), url.Values{
		"api-version": {computeAPIVersion},
	}, map[string]any{
		"location": location,
		"tags":     tags,
		"properties": map[string]any{
			"hardwareProfile": map[string]any{
				"vmSize": size,
			},
			"storageProfile": map[string]any{
				"imageReference": map[string]any{
					"publisher": image.Publisher,
					"offer":     image.Offer,
					"sku":       image.SKU,
					"version":   image.Version,
				},
				"osDisk": map[string]any{
					"createOption": "FromImage",
					"name":         osDiskName,
					"deleteOption": "Delete",
					"managedDisk": map[string]any{
						"storageAccountType": "StandardSSD_LRS",
					},
				},
			},
			"osProfile": osProfile,
			"networkProfile": map[string]any{
				"networkInterfaces": []map[string]any{
					{
						"id": c.networkInterfacePath(resourceGroup, nicName),
						"properties": map[string]any{
							"primary":      true,
							"deleteOption": "Delete",
						},
					},
				},
			},
		},
	})
}

func buildInstance(resource virtualMachineResource, networkInterfaces []NetworkInterface) Instance {
	resourceGroup := resourceGroupFromID(resource.ID)
	privateIPs := make([]string, 0)
	publicIPs := make([]string, 0)
	for _, nic := range networkInterfaces {
		privateIPs = append(privateIPs, nic.PrivateIPs...)
		publicIPs = append(publicIPs, nic.PublicIPs...)
	}

	osType := ""
	if resource.Properties.StorageProfile.OSDisk != nil {
		osType = strings.TrimSpace(resource.Properties.StorageProfile.OSDisk.OSType)
	}

	return Instance{
		InstanceID:        EncodeInstanceID(resourceGroup, resource.Name),
		ResourceID:        strings.TrimSpace(resource.ID),
		Name:              strings.TrimSpace(resource.Name),
		ResourceGroup:     resourceGroup,
		Location:          normalizeLocation(resource.Location),
		Size:              strings.TrimSpace(resource.Properties.HardwareProfile.VMSize),
		ProvisioningState: strings.TrimSpace(resource.Properties.ProvisioningState),
		PowerState:        powerStateFromStatuses(resource.Properties.InstanceView),
		ComputerName:      strings.TrimSpace(computerName(resource)),
		OsType:            osType,
		Image:             imageLabel(resource),
		PrivateIPs:        uniqueStrings(privateIPs),
		PublicIPs:         uniqueStrings(publicIPs),
		Tags:              normalizeTags(resource.Tags),
	}
}

func resourceGroupFromID(resourceID string) string {
	const marker = "/resourceGroups/"
	index := strings.Index(resourceID, marker)
	if index == -1 {
		return ""
	}
	remaining := resourceID[index+len(marker):]
	next := strings.Index(remaining, "/")
	if next == -1 {
		return strings.TrimSpace(remaining)
	}
	return strings.TrimSpace(remaining[:next])
}

func parentAzureResourceID(resourceID, childMarker string) string {
	resourceID = strings.TrimSpace(resourceID)
	childMarker = strings.TrimSpace(childMarker)
	if resourceID == "" || childMarker == "" {
		return ""
	}
	index := strings.LastIndex(strings.ToLower(resourceID), strings.ToLower(childMarker))
	if index <= 0 {
		return ""
	}
	return strings.TrimSpace(resourceID[:index])
}

func toManagementPath(resourceID string) (string, error) {
	resourceID = strings.TrimSpace(resourceID)
	if resourceID == "" {
		return "", errors.New("azure resource id is empty")
	}
	if strings.HasPrefix(resourceID, "https://management.azure.com/") {
		parsed, err := url.Parse(resourceID)
		if err != nil {
			return "", err
		}
		resourceID = parsed.Path
	}
	if !strings.HasPrefix(resourceID, "/") {
		return "", errors.New("azure resource id is invalid")
	}
	return resourceID, nil
}

func isKomariManagedTags(tags map[string]string) bool {
	for key, value := range tags {
		if strings.EqualFold(strings.TrimSpace(key), "managed-by") && strings.EqualFold(strings.TrimSpace(value), "komari") {
			return true
		}
	}
	return false
}

func isAzureNotFoundError(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		if apiErr.StatusCode == http.StatusNotFound {
			return true
		}
		code := strings.ToLower(strings.TrimSpace(apiErr.Code))
		return code == "resourcenotfound" || code == "notfound" || code == "resourcegroupnotfound"
	}
	return false
}

func mapFromAny(value any) map[string]any {
	if value == nil {
		return nil
	}
	switch typed := value.(type) {
	case map[string]any:
		return typed
	default:
		return nil
	}
}

func sliceFromAny(value any) []any {
	switch typed := value.(type) {
	case []any:
		return typed
	default:
		return nil
	}
}

func stringFromAny(value any) string {
	if value == nil {
		return ""
	}
	switch typed := value.(type) {
	case string:
		return typed
	case fmt.Stringer:
		return typed.String()
	default:
		return strings.TrimSpace(fmt.Sprintf("%v", value))
	}
}

func boolFromAny(value any) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(typed))
		return err == nil && parsed
	default:
		return false
	}
}

func anyMapString(value any) map[string]string {
	rawMap := mapFromAny(value)
	if rawMap == nil {
		return nil
	}
	result := make(map[string]string, len(rawMap))
	for key, raw := range rawMap {
		normalizedKey := strings.TrimSpace(key)
		if normalizedKey == "" {
			continue
		}
		result[normalizedKey] = strings.TrimSpace(stringFromAny(raw))
	}
	return result
}

func canRetryVirtualMachineListWithoutExpand(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "expand") || strings.Contains(message, "instanceview")
}

func parseAzureAPIError(statusCode int, payload []byte) error {
	var envelope armErrorEnvelope
	if err := json.Unmarshal(payload, &envelope); err == nil && envelope.Error != nil {
		return &APIError{
			StatusCode: statusCode,
			Code:       strings.TrimSpace(envelope.Error.Code),
			Message:    strings.TrimSpace(envelope.Error.Message),
		}
	}

	var token tokenResponse
	if err := json.Unmarshal(payload, &token); err == nil && (token.Error != "" || token.ErrorDescription != "") {
		return &APIError{
			StatusCode: statusCode,
			Code:       strings.TrimSpace(token.Error),
			Message:    strings.TrimSpace(token.ErrorDescription),
		}
	}

	return &APIError{
		StatusCode: statusCode,
		Message:    strings.TrimSpace(string(bytes.TrimSpace(payload))),
	}
}

func parseAzureProvisioningError(payload []byte, fallbackState string) error {
	var status armAsyncOperationStatus
	if err := json.Unmarshal(payload, &status); err == nil && status.Error != nil {
		return &APIError{
			StatusCode: http.StatusBadGateway,
			Code:       strings.TrimSpace(status.Error.Code),
			Message:    strings.TrimSpace(status.Error.Message),
		}
	}

	message := strings.TrimSpace(fallbackState)
	if message == "" {
		message = "azure resource provisioning failed"
	}
	return &APIError{
		StatusCode: http.StatusBadGateway,
		Message:    message,
	}
}

func requiresAsyncPolling(statusCode int, headers http.Header, payload []byte) bool {
	if statusCode == http.StatusAccepted {
		return true
	}
	if strings.TrimSpace(headers.Get("Azure-AsyncOperation")) != "" || strings.TrimSpace(headers.Get("Location")) != "" {
		return true
	}

	switch strings.ToLower(strings.TrimSpace(extractProvisioningState(payload))) {
	case "creating", "updating", "deleting", "accepted", "running":
		return true
	default:
		return false
	}
}

func interpretAzureAsyncResponse(response *armResponse) (bool, time.Duration, error) {
	if response == nil {
		return false, 0, errors.New("azure async poll response is missing")
	}

	delay := retryAfterDelay(response.Headers.Get("Retry-After"))
	var status armAsyncOperationStatus
	if len(bytes.TrimSpace(response.Body)) > 0 && json.Unmarshal(response.Body, &status) == nil {
		switch strings.ToLower(strings.TrimSpace(status.Status)) {
		case "succeeded":
			return true, 0, nil
		case "failed", "canceled":
			if status.Error != nil {
				return false, 0, &APIError{
					StatusCode: http.StatusBadGateway,
					Code:       strings.TrimSpace(status.Error.Code),
					Message:    strings.TrimSpace(status.Error.Message),
				}
			}
			return false, 0, &APIError{
				StatusCode: http.StatusBadGateway,
				Message:    strings.TrimSpace(status.Status),
			}
		case "", "inprogress", "running", "accepted":
		default:
			return false, delay, nil
		}

		switch strings.ToLower(strings.TrimSpace(firstNonEmpty(status.Status, extractProvisioningState(response.Body)))) {
		case "succeeded":
			return true, 0, nil
		case "failed", "canceled":
			return false, 0, parseAzureProvisioningError(response.Body, firstNonEmpty(status.Status, extractProvisioningState(response.Body)))
		}
	}

	if response.StatusCode == http.StatusAccepted {
		return false, delay, nil
	}
	if response.StatusCode == http.StatusOK || response.StatusCode == http.StatusCreated || response.StatusCode == http.StatusNoContent {
		return true, 0, nil
	}
	return false, delay, nil
}

func retryAfterDelay(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func waitForAzureRetry(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		delay = 3 * time.Second
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func extractProvisioningState(payload []byte) string {
	if len(bytes.TrimSpace(payload)) == 0 {
		return ""
	}

	var envelope struct {
		Properties *struct {
			ProvisioningState string `json:"provisioningState"`
		} `json:"properties"`
		Status string `json:"status"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return ""
	}
	provisioningState := ""
	if envelope.Properties != nil {
		provisioningState = envelope.Properties.ProvisioningState
	}
	return strings.TrimSpace(firstNonEmpty(provisioningState, envelope.Status))
}

func normalizeAzureResourceName(value string, maxLen int, fallback string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		value = strings.ToLower(strings.TrimSpace(fallback))
	}

	var builder strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
			lastDash = false
		case r >= '0' && r <= '9':
			builder.WriteRune(r)
			lastDash = false
		case r == '-' || r == '_' || r == '.' || r == ' ':
			if builder.Len() == 0 || lastDash {
				continue
			}
			builder.WriteRune('-')
			lastDash = true
		}
	}

	normalized := strings.Trim(builder.String(), "-")
	if maxLen > 0 && len(normalized) > maxLen {
		normalized = strings.Trim(normalized[:maxLen], "-")
	}
	if normalized == "" {
		normalized = strings.Trim(normalizeAzureResourceName(fallback, maxLen, "komari"), "-")
	}
	return normalized
}

func normalizeAzureLinuxUsername(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}

	var builder strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			builder.WriteRune(r)
		case r >= '0' && r <= '9':
			if builder.Len() == 0 {
				continue
			}
			builder.WriteRune(r)
		case r == '-' || r == '_':
			if builder.Len() == 0 {
				continue
			}
			builder.WriteRune(r)
		}
		if builder.Len() >= 32 {
			break
		}
	}

	return strings.Trim(builder.String(), "-_")
}

func skuCapabilityInt(capabilities []struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}, name string) int {
	for _, capability := range capabilities {
		if !strings.EqualFold(strings.TrimSpace(capability.Name), strings.TrimSpace(name)) {
			continue
		}
		value, err := strconv.Atoi(strings.TrimSpace(capability.Value))
		if err == nil {
			return value
		}
	}
	return 0
}

func skuCapabilityFloat(capabilities []struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}, name string) float64 {
	for _, capability := range capabilities {
		if !strings.EqualFold(strings.TrimSpace(capability.Name), strings.TrimSpace(name)) {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(capability.Value), 64)
		if err == nil {
			return value
		}
	}
	return 0
}

func skuRestrictedForLocation(resource resourceSKU, location string) bool {
	location = normalizeLocation(location)
	if location == "" {
		return false
	}

	for _, restriction := range resource.Restrictions {
		if !strings.EqualFold(strings.TrimSpace(restriction.Type), "Location") {
			continue
		}
		if restriction.RestrictionInfo == nil {
			return true
		}
		for _, restrictedLocation := range restriction.RestrictionInfo.Locations {
			if normalizeLocation(restrictedLocation) == location {
				return true
			}
		}
	}
	return false
}

func skuZonesForLocation(resource resourceSKU, location string) []string {
	location = normalizeLocation(location)
	for _, item := range resource.LocationInfo {
		if normalizeLocation(item.Location) != location {
			continue
		}
		return uniqueStrings(item.Zones)
	}
	return []string{}
}

func powerStateFromStatuses(view *struct {
	Statuses []instanceStatus `json:"statuses"`
}) string {
	if view == nil {
		return ""
	}
	for _, status := range view.Statuses {
		code := strings.TrimSpace(status.Code)
		if strings.HasPrefix(strings.ToLower(code), "powerstate/") {
			return strings.TrimPrefix(code, "PowerState/")
		}
	}
	return ""
}

func computerName(resource virtualMachineResource) string {
	if resource.Properties.OSProfile == nil {
		return ""
	}
	return resource.Properties.OSProfile.ComputerName
}

func imageLabel(resource virtualMachineResource) string {
	ref := resource.Properties.StorageProfile.ImageReference
	parts := make([]string, 0, 4)
	for _, value := range []string{ref.Publisher, ref.Offer, ref.SKU, ref.Version} {
		value = strings.TrimSpace(value)
		if value != "" {
			parts = append(parts, value)
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, " / ")
	}
	return firstNonEmpty(ref.ID, ref.SharedGalleryImageID, ref.CommunityGalleryImageID)
}

func uniqueStrings(values []string) []string {
	if len(values) == 0 {
		return []string{}
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func normalizeTags(tags map[string]string) map[string]string {
	if len(tags) == 0 {
		return map[string]string{}
	}
	result := make(map[string]string, len(tags))
	for key, value := range tags {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		result[key] = strings.TrimSpace(value)
	}
	return result
}

func osManagedDiskID(disk *struct {
	OSType       string `json:"osType"`
	Name         string `json:"name"`
	DiskSizeGB   int    `json:"diskSizeGB"`
	CreateOption string `json:"createOption"`
	ManagedDisk  *struct {
		ID                 string `json:"id"`
		StorageAccountType string `json:"storageAccountType"`
	} `json:"managedDisk"`
}) string {
	if disk == nil || disk.ManagedDisk == nil {
		return ""
	}
	return strings.TrimSpace(disk.ManagedDisk.ID)
}

func osManagedDiskType(disk *struct {
	OSType       string `json:"osType"`
	Name         string `json:"name"`
	DiskSizeGB   int    `json:"diskSizeGB"`
	CreateOption string `json:"createOption"`
	ManagedDisk  *struct {
		ID                 string `json:"id"`
		StorageAccountType string `json:"storageAccountType"`
	} `json:"managedDisk"`
}) string {
	if disk == nil || disk.ManagedDisk == nil {
		return ""
	}
	return strings.TrimSpace(disk.ManagedDisk.StorageAccountType)
}

func dataManagedDiskID(disk struct {
	Lun          int    `json:"lun"`
	Name         string `json:"name"`
	DiskSizeGB   int    `json:"diskSizeGB"`
	CreateOption string `json:"createOption"`
	ManagedDisk  *struct {
		ID                 string `json:"id"`
		StorageAccountType string `json:"storageAccountType"`
	} `json:"managedDisk"`
}) string {
	if disk.ManagedDisk == nil {
		return ""
	}
	return strings.TrimSpace(disk.ManagedDisk.ID)
}

func dataManagedDiskType(disk struct {
	Lun          int    `json:"lun"`
	Name         string `json:"name"`
	DiskSizeGB   int    `json:"diskSizeGB"`
	CreateOption string `json:"createOption"`
	ManagedDisk  *struct {
		ID                 string `json:"id"`
		StorageAccountType string `json:"storageAccountType"`
	} `json:"managedDisk"`
}) string {
	if disk.ManagedDisk == nil {
		return ""
	}
	return strings.TrimSpace(disk.ManagedDisk.StorageAccountType)
}

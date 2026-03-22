package failover

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	awscloud "github.com/komari-monitor/komari/utils/cloudprovider/aws"
	"github.com/komari-monitor/komari/utils/cloudprovider/digitalocean"
	linodecloud "github.com/komari-monitor/komari/utils/cloudprovider/linode"
)

type CatalogOption struct {
	Value string `json:"value"`
	Label string `json:"label"`
	Hint  string `json:"hint,omitempty"`
}

type PlanCatalog struct {
	Provider          string          `json:"provider"`
	ActionType        string          `json:"action_type"`
	Service           string          `json:"service,omitempty"`
	Region            string          `json:"region,omitempty"`
	Regions           []CatalogOption `json:"regions"`
	AvailabilityZones []CatalogOption `json:"availability_zones"`
	Images            []CatalogOption `json:"images"`
	InstanceTypes     []CatalogOption `json:"instance_types"`
	KeyPairs          []CatalogOption `json:"key_pairs"`
	Subnets           []CatalogOption `json:"subnets"`
	SecurityGroups    []CatalogOption `json:"security_groups"`
	Bundles           []CatalogOption `json:"bundles"`
	Blueprints        []CatalogOption `json:"blueprints"`
	Sizes             []CatalogOption `json:"sizes"`
	Types             []CatalogOption `json:"types"`
}

func LoadPlanCatalog(userUUID, providerName, entryID, actionType, service, region string) (*PlanCatalog, error) {
	providerName = strings.ToLower(strings.TrimSpace(providerName))
	actionType = strings.ToLower(strings.TrimSpace(actionType))
	service = strings.ToLower(strings.TrimSpace(service))
	region = strings.TrimSpace(region)

	switch providerName {
	case "aws":
		return loadAWSPlanCatalog(userUUID, entryID, actionType, service, region)
	case "digitalocean":
		return loadDigitalOceanPlanCatalog(userUUID, entryID, actionType)
	case "linode":
		return loadLinodePlanCatalog(userUUID, entryID, actionType)
	default:
		return nil, fmt.Errorf("unsupported failover plan provider: %s", providerName)
	}
}

func loadAWSPlanCatalog(userUUID, entryID, actionType, service, region string) (*PlanCatalog, error) {
	addition, credential, err := loadAWSCredential(userUUID, entryID)
	if err != nil {
		return nil, err
	}
	if service == "" {
		service = "ec2"
	}

	resolvedRegion := region
	if resolvedRegion == "" {
		resolvedRegion = strings.TrimSpace(addition.ActiveRegion)
	}
	if resolvedRegion == "" {
		resolvedRegion = strings.TrimSpace(credential.DefaultRegion)
	}
	if resolvedRegion == "" {
		resolvedRegion = awscloud.DefaultRegion
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	catalog := &PlanCatalog{
		Provider:          "aws",
		ActionType:        actionType,
		Service:           service,
		Region:            resolvedRegion,
		Regions:           []CatalogOption{},
		AvailabilityZones: []CatalogOption{},
		Images:            []CatalogOption{},
		InstanceTypes:     []CatalogOption{},
		KeyPairs:          []CatalogOption{},
		Subnets:           []CatalogOption{},
		SecurityGroups:    []CatalogOption{},
		Bundles:           []CatalogOption{},
		Blueprints:        []CatalogOption{},
		Sizes:             []CatalogOption{},
		Types:             []CatalogOption{},
	}

	switch service {
	case "", "ec2":
		regions, err := awscloud.ListRegions(ctx, credential)
		if err != nil {
			return nil, err
		}
		instanceTypes, err := awscloud.ListInstanceTypes(ctx, credential, resolvedRegion)
		if err != nil {
			return nil, err
		}
		images, err := awscloud.ListSuggestedImages(ctx, credential, resolvedRegion)
		if err != nil {
			return nil, err
		}
		keyPairs, err := awscloud.ListKeyPairs(ctx, credential, resolvedRegion)
		if err != nil {
			return nil, err
		}
		subnets, err := awscloud.ListSubnets(ctx, credential, resolvedRegion)
		if err != nil {
			return nil, err
		}
		securityGroups, err := awscloud.ListSecurityGroups(ctx, credential, resolvedRegion)
		if err != nil {
			return nil, err
		}

		for _, item := range regions {
			catalog.Regions = append(catalog.Regions, CatalogOption{
				Value: strings.TrimSpace(item.Name),
				Label: strings.TrimSpace(item.Name),
				Hint:  strings.TrimSpace(item.Endpoint),
			})
		}
		for _, item := range instanceTypes {
			catalog.InstanceTypes = append(catalog.InstanceTypes, CatalogOption{
				Value: strings.TrimSpace(item.Name),
				Label: strings.TrimSpace(item.Name),
				Hint: fmt.Sprintf(
					"%d vCPU · %.1f GiB",
					item.VCpus,
					float64(item.MemoryMiB)/1024.0,
				),
			})
		}
		for _, item := range images {
			label := firstNonEmpty(strings.TrimSpace(item.Name), strings.TrimSpace(item.ImageID))
			hintParts := make([]string, 0, 2)
			if imageID := strings.TrimSpace(item.ImageID); imageID != "" {
				hintParts = append(hintParts, imageID)
			}
			if platform := strings.TrimSpace(item.PlatformDetails); platform != "" {
				hintParts = append(hintParts, platform)
			}
			catalog.Images = append(catalog.Images, CatalogOption{
				Value: strings.TrimSpace(item.ImageID),
				Label: label,
				Hint:  strings.Join(hintParts, " · "),
			})
		}
		for _, item := range keyPairs {
			catalog.KeyPairs = append(catalog.KeyPairs, CatalogOption{
				Value: strings.TrimSpace(item.KeyName),
				Label: strings.TrimSpace(item.KeyName),
				Hint:  strings.TrimSpace(item.KeyType),
			})
		}
		for _, item := range subnets {
			catalog.Subnets = append(catalog.Subnets, CatalogOption{
				Value: strings.TrimSpace(item.SubnetID),
				Label: strings.TrimSpace(item.SubnetID),
				Hint:  fmt.Sprintf("%s · %s", strings.TrimSpace(item.AvailabilityZone), strings.TrimSpace(item.CidrBlock)),
			})
		}
		for _, item := range securityGroups {
			catalog.SecurityGroups = append(catalog.SecurityGroups, CatalogOption{
				Value: strings.TrimSpace(item.GroupID),
				Label: firstNonEmpty(strings.TrimSpace(item.GroupName), strings.TrimSpace(item.GroupID)),
				Hint:  strings.TrimSpace(item.Description),
			})
		}
	case "lightsail":
		regions, err := awscloud.ListLightsailRegions(ctx, credential, resolvedRegion)
		if err != nil {
			return nil, err
		}
		bundles, err := awscloud.ListLightsailBundles(ctx, credential, resolvedRegion)
		if err != nil {
			return nil, err
		}
		blueprints, err := awscloud.ListLightsailBlueprints(ctx, credential, resolvedRegion)
		if err != nil {
			return nil, err
		}
		keyPairs, err := awscloud.ListLightsailKeyPairs(ctx, credential, resolvedRegion)
		if err != nil {
			return nil, err
		}

		for _, item := range regions {
			catalog.Regions = append(catalog.Regions, CatalogOption{
				Value: strings.TrimSpace(item.Name),
				Label: firstNonEmpty(strings.TrimSpace(item.DisplayName), strings.TrimSpace(item.Name)),
				Hint:  strings.TrimSpace(item.Description),
			})
			if strings.TrimSpace(item.Name) == resolvedRegion {
				for _, zone := range item.AvailabilityZones {
					catalog.AvailabilityZones = append(catalog.AvailabilityZones, CatalogOption{
						Value: strings.TrimSpace(zone.Name),
						Label: strings.TrimSpace(zone.Name),
						Hint:  strings.TrimSpace(zone.State),
					})
				}
			}
		}
		for _, item := range bundles {
			catalog.Bundles = append(catalog.Bundles, CatalogOption{
				Value: strings.TrimSpace(item.BundleID),
				Label: firstNonEmpty(strings.TrimSpace(item.Name), strings.TrimSpace(item.BundleID)),
				Hint: fmt.Sprintf(
					"%d CPU · %.1f GiB · %d GB",
					item.CPUCount,
					item.RAMSizeInGB,
					item.DiskSizeInGB,
				),
			})
		}
		for _, item := range blueprints {
			catalog.Blueprints = append(catalog.Blueprints, CatalogOption{
				Value: strings.TrimSpace(item.BlueprintID),
				Label: firstNonEmpty(strings.TrimSpace(item.Name), strings.TrimSpace(item.BlueprintID)),
				Hint:  firstNonEmpty(strings.TrimSpace(item.Platform), strings.TrimSpace(item.Description)),
			})
		}
		for _, item := range keyPairs {
			catalog.KeyPairs = append(catalog.KeyPairs, CatalogOption{
				Value: strings.TrimSpace(item.Name),
				Label: strings.TrimSpace(item.Name),
			})
		}
	default:
		return nil, fmt.Errorf("unsupported aws failover service: %s", service)
	}

	return catalog, nil
}

func loadDigitalOceanPlanCatalog(userUUID, entryID, actionType string) (*PlanCatalog, error) {
	_, token, err := loadDigitalOceanToken(userUUID, entryID)
	if err != nil {
		return nil, err
	}
	client, err := digitalocean.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	regions, err := client.ListRegions(ctx)
	if err != nil {
		return nil, err
	}
	sizes, err := client.ListSizes(ctx)
	if err != nil {
		return nil, err
	}
	images, err := client.ListImages(ctx, "distribution")
	if err != nil {
		return nil, err
	}

	sort.Slice(regions, func(i, j int) bool {
		return regions[i].Slug < regions[j].Slug
	})
	sort.Slice(sizes, func(i, j int) bool {
		return sizes[i].PriceMonthly < sizes[j].PriceMonthly
	})
	sort.Slice(images, func(i, j int) bool {
		left := strings.ToLower(strings.TrimSpace(images[i].Distribution + " " + images[i].Name))
		right := strings.ToLower(strings.TrimSpace(images[j].Distribution + " " + images[j].Name))
		return left < right
	})

	catalog := &PlanCatalog{
		Provider:          "digitalocean",
		ActionType:        actionType,
		Regions:           []CatalogOption{},
		AvailabilityZones: []CatalogOption{},
		Images:            []CatalogOption{},
		InstanceTypes:     []CatalogOption{},
		KeyPairs:          []CatalogOption{},
		Subnets:           []CatalogOption{},
		SecurityGroups:    []CatalogOption{},
		Bundles:           []CatalogOption{},
		Blueprints:        []CatalogOption{},
		Sizes:             []CatalogOption{},
		Types:             []CatalogOption{},
	}
	for _, item := range regions {
		catalog.Regions = append(catalog.Regions, CatalogOption{
			Value: strings.TrimSpace(item.Slug),
			Label: firstNonEmpty(strings.TrimSpace(item.Name), strings.TrimSpace(item.Slug)),
		})
	}
	for _, item := range sizes {
		catalog.Sizes = append(catalog.Sizes, CatalogOption{
			Value: strings.TrimSpace(item.Slug),
			Label: strings.TrimSpace(item.Slug),
			Hint: fmt.Sprintf(
				"%d vCPU · %d MB · $%.2f/mo",
				item.Vcpus,
				item.Memory,
				item.PriceMonthly,
			),
		})
	}
	for _, item := range images {
		imageValue := strings.TrimSpace(item.Slug)
		if imageValue == "" {
			imageValue = strconv.Itoa(item.ID)
		}
		catalog.Images = append(catalog.Images, CatalogOption{
			Value: imageValue,
			Label: firstNonEmpty(strings.TrimSpace(item.Distribution+" "+item.Name), strings.TrimSpace(item.Name), imageValue),
			Hint:  strings.TrimSpace(item.Description),
		})
	}
	return catalog, nil
}

func loadLinodePlanCatalog(userUUID, entryID, actionType string) (*PlanCatalog, error) {
	_, token, err := loadLinodeToken(userUUID, entryID)
	if err != nil {
		return nil, err
	}
	client, err := linodecloud.NewClientFromToken(token.Token)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	regions, err := client.ListRegions(ctx)
	if err != nil {
		return nil, err
	}
	types, err := client.ListTypes(ctx)
	if err != nil {
		return nil, err
	}
	images, err := client.ListImages(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(regions, func(i, j int) bool {
		return regions[i].ID < regions[j].ID
	})
	sort.Slice(types, func(i, j int) bool {
		return types[i].Price.Monthly < types[j].Price.Monthly
	})
	sort.Slice(images, func(i, j int) bool {
		left := strings.ToLower(strings.TrimSpace(images[i].Label))
		right := strings.ToLower(strings.TrimSpace(images[j].Label))
		return left < right
	})

	catalog := &PlanCatalog{
		Provider:          "linode",
		ActionType:        actionType,
		Regions:           []CatalogOption{},
		AvailabilityZones: []CatalogOption{},
		Images:            []CatalogOption{},
		InstanceTypes:     []CatalogOption{},
		KeyPairs:          []CatalogOption{},
		Subnets:           []CatalogOption{},
		SecurityGroups:    []CatalogOption{},
		Bundles:           []CatalogOption{},
		Blueprints:        []CatalogOption{},
		Sizes:             []CatalogOption{},
		Types:             []CatalogOption{},
	}
	for _, item := range regions {
		catalog.Regions = append(catalog.Regions, CatalogOption{
			Value: strings.TrimSpace(item.ID),
			Label: firstNonEmpty(strings.TrimSpace(item.Label), strings.TrimSpace(item.ID)),
			Hint:  strings.TrimSpace(item.Country),
		})
	}
	for _, item := range types {
		catalog.Types = append(catalog.Types, CatalogOption{
			Value: strings.TrimSpace(item.ID),
			Label: firstNonEmpty(strings.TrimSpace(item.Label), strings.TrimSpace(item.ID)),
			Hint: fmt.Sprintf(
				"%d vCPU · %d MB · $%.2f/mo",
				item.VCPUs,
				item.Memory,
				item.Price.Monthly,
			),
		})
	}
	for _, item := range images {
		catalog.Images = append(catalog.Images, CatalogOption{
			Value: strings.TrimSpace(item.ID),
			Label: firstNonEmpty(strings.TrimSpace(item.Label), strings.TrimSpace(item.ID)),
			Hint:  firstNonEmpty(strings.TrimSpace(item.Description), strings.TrimSpace(item.Vendor)),
		})
	}
	return catalog, nil
}

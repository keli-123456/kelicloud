package linode

import (
	"context"
	"fmt"
	"strings"
)

func hasCapability(capabilities []string, target string) bool {
	target = strings.ToLower(strings.TrimSpace(target))
	if target == "" {
		return false
	}

	for _, capability := range capabilities {
		if strings.ToLower(strings.TrimSpace(capability)) == target {
			return true
		}
	}
	return false
}

func RegionSupportsMetadata(region *Region) bool {
	return region != nil && hasCapability(region.Capabilities, "metadata")
}

func ImageSupportsCloudInit(image *Image) bool {
	return image != nil && hasCapability(image.Capabilities, "cloud-init")
}

func FindRegionByID(regions []Region, regionID string) *Region {
	regionID = strings.TrimSpace(regionID)
	if regionID == "" {
		return nil
	}

	for index := range regions {
		if strings.TrimSpace(regions[index].ID) == regionID {
			return &regions[index]
		}
	}
	return nil
}

func FindImageByID(images []Image, imageID string) *Image {
	imageID = strings.TrimSpace(imageID)
	if imageID == "" {
		return nil
	}

	for index := range images {
		if strings.TrimSpace(images[index].ID) == imageID {
			return &images[index]
		}
	}
	return nil
}

func ValidateAutoConnectSupport(ctx context.Context, client *Client, regionID, imageID string) error {
	if client == nil {
		return fmt.Errorf("linode client is required")
	}

	regionID = strings.TrimSpace(regionID)
	imageID = strings.TrimSpace(imageID)
	if regionID == "" || imageID == "" {
		return nil
	}

	regions, err := client.ListRegions(ctx)
	if err != nil {
		return fmt.Errorf("failed to load Linode regions for auto-connect validation: %w", err)
	}
	region := FindRegionByID(regions, regionID)
	if region == nil {
		return fmt.Errorf("selected Linode region %q was not found", regionID)
	}
	if !RegionSupportsMetadata(region) {
		return fmt.Errorf(
			"selected Linode region %q does not support Metadata; automatic agent auto-connect requires a Metadata-compatible region",
			regionID,
		)
	}

	images, err := client.ListImages(ctx)
	if err != nil {
		return fmt.Errorf("failed to load Linode images for auto-connect validation: %w", err)
	}
	image := FindImageByID(images, imageID)
	if image == nil {
		return fmt.Errorf("selected Linode image %q was not found", imageID)
	}
	if !ImageSupportsCloudInit(image) {
		return fmt.Errorf(
			"selected Linode image %q does not support cloud-init; automatic agent auto-connect requires a cloud-init-compatible image",
			imageID,
		)
	}

	return nil
}

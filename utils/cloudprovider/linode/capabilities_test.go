package linode

import "testing"

func TestRegionSupportsMetadata(t *testing.T) {
	region := &Region{
		ID:           "us-east",
		Capabilities: []string{"Backups", "Metadata"},
	}

	if !RegionSupportsMetadata(region) {
		t.Fatal("expected region metadata capability to be detected")
	}
}

func TestImageSupportsCloudInit(t *testing.T) {
	image := &Image{
		ID:           "linode/ubuntu24.04",
		Capabilities: []string{"cloud-init", "distributed-sites"},
	}

	if !ImageSupportsCloudInit(image) {
		t.Fatal("expected image cloud-init capability to be detected")
	}
}

func TestFindRegionByIDReturnsMatch(t *testing.T) {
	regions := []Region{
		{ID: "us-east"},
		{ID: "us-west"},
	}

	region := FindRegionByID(regions, "us-west")
	if region == nil {
		t.Fatal("expected region to be found")
	}
	if region.ID != "us-west" {
		t.Fatalf("expected us-west, got %q", region.ID)
	}
}

func TestFindImageByIDReturnsMatch(t *testing.T) {
	images := []Image{
		{ID: "linode/debian12"},
		{ID: "linode/ubuntu24.04"},
	}

	image := FindImageByID(images, "linode/ubuntu24.04")
	if image == nil {
		t.Fatal("expected image to be found")
	}
	if image.ID != "linode/ubuntu24.04" {
		t.Fatalf("expected linode/ubuntu24.04, got %q", image.ID)
	}
}

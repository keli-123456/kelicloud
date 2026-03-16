package database

import (
	"errors"
	"testing"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func TestCreateAndListTenantInvites(t *testing.T) {
	db := setupTenantTestDB(t, &models.User{}, &models.Tenant{}, &models.TenantMember{}, &models.TenantInvite{})

	inviter := createTenantTestUser(t, db, "user-inviter", "inviter")
	tenant, err := createTenantWithDB(db, "Invite Workspace", "", "", inviter.UUID)
	if err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	expiresAt := time.Now().Add(48 * time.Hour)
	invite, err := createTenantInviteWithDB(db, tenant.ID, inviter.UUID, RoleAdmin, &expiresAt)
	if err != nil {
		t.Fatalf("failed to create tenant invite: %v", err)
	}
	if invite.Token == "" {
		t.Fatal("expected invite token to be generated")
	}

	invites, err := listTenantInvitesWithDB(db, tenant.ID)
	if err != nil {
		t.Fatalf("failed to list tenant invites: %v", err)
	}
	if len(invites) != 1 {
		t.Fatalf("expected one active invite, got %d", len(invites))
	}
	if invites[0].ID != invite.ID || invites[0].Role != RoleAdmin {
		t.Fatalf("unexpected invite payload: %+v", invites[0])
	}
}

func TestAcceptTenantInviteWithDBAddsMembershipAndMarksInviteAccepted(t *testing.T) {
	db := setupTenantTestDB(t, &models.User{}, &models.Tenant{}, &models.TenantMember{}, &models.TenantInvite{})

	inviter := createTenantTestUser(t, db, "user-inviter", "inviter")
	invitee := createTenantTestUser(t, db, "user-invitee", "invitee")
	tenant, err := createTenantWithDB(db, "Accept Workspace", "", "", inviter.UUID)
	if err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	expiresAt := time.Now().Add(24 * time.Hour)
	invite, err := createTenantInviteWithDB(db, tenant.ID, inviter.UUID, RoleOperator, &expiresAt)
	if err != nil {
		t.Fatalf("failed to create tenant invite: %v", err)
	}

	acceptedTenant, err := acceptTenantInviteWithDB(db, invite.Token, invitee.UUID)
	if err != nil {
		t.Fatalf("acceptTenantInviteWithDB returned error: %v", err)
	}
	if acceptedTenant == nil || acceptedTenant.ID != tenant.ID {
		t.Fatalf("expected accepted tenant %q, got %+v", tenant.ID, acceptedTenant)
	}

	member, err := getTenantMemberWithDB(db, tenant.ID, invitee.UUID)
	if err != nil {
		t.Fatalf("failed to load accepted tenant member: %v", err)
	}
	if member.Role != RoleOperator {
		t.Fatalf("expected invitee role %q, got %q", RoleOperator, member.Role)
	}

	var storedInvite models.TenantInvite
	if err := db.Where("id = ?", invite.ID).First(&storedInvite).Error; err != nil {
		t.Fatalf("failed to reload tenant invite: %v", err)
	}
	if storedInvite.AcceptedAt == nil || storedInvite.AcceptedBy != invitee.UUID {
		t.Fatalf("expected invite to be marked accepted, got %+v", storedInvite)
	}

	invites, err := listTenantInvitesWithDB(db, tenant.ID)
	if err != nil {
		t.Fatalf("failed to list tenant invites after accept: %v", err)
	}
	if len(invites) != 0 {
		t.Fatalf("expected accepted invite to disappear from active list, got %+v", invites)
	}
}

func TestAcceptTenantInviteWithDBRejectsExpiredInvite(t *testing.T) {
	db := setupTenantTestDB(t, &models.User{}, &models.Tenant{}, &models.TenantMember{}, &models.TenantInvite{})

	inviter := createTenantTestUser(t, db, "user-inviter", "inviter")
	invitee := createTenantTestUser(t, db, "user-invitee", "invitee")
	tenant, err := createTenantWithDB(db, "Expired Workspace", "", "", inviter.UUID)
	if err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	expiredAt := time.Now().Add(-time.Hour)
	invite, err := createTenantInviteWithDB(db, tenant.ID, inviter.UUID, RoleViewer, &expiredAt)
	if err != nil {
		t.Fatalf("failed to create expired invite: %v", err)
	}

	_, err = acceptTenantInviteWithDB(db, invite.Token, invitee.UUID)
	if !errors.Is(err, ErrTenantInviteExpired) {
		t.Fatalf("expected ErrTenantInviteExpired, got %v", err)
	}
}

func TestRevokeTenantInviteWithDBDeletesInvite(t *testing.T) {
	db := setupTenantTestDB(t, &models.User{}, &models.Tenant{}, &models.TenantMember{}, &models.TenantInvite{})

	inviter := createTenantTestUser(t, db, "user-inviter", "inviter")
	tenant, err := createTenantWithDB(db, "Revoke Workspace", "", "", inviter.UUID)
	if err != nil {
		t.Fatalf("failed to create tenant: %v", err)
	}

	invite, err := createTenantInviteWithDB(db, tenant.ID, inviter.UUID, RoleViewer, nil)
	if err != nil {
		t.Fatalf("failed to create tenant invite: %v", err)
	}

	if err := revokeTenantInviteWithDB(db, tenant.ID, invite.ID); err != nil {
		t.Fatalf("revokeTenantInviteWithDB returned error: %v", err)
	}

	var count int64
	if err := db.Model(&models.TenantInvite{}).Where("id = ?", invite.ID).Count(&count).Error; err != nil {
		t.Fatalf("failed to count tenant invites: %v", err)
	}
	if count != 0 {
		t.Fatalf("expected revoked invite to be deleted, found %d rows", count)
	}

	if err := revokeTenantInviteWithDB(db, tenant.ID, invite.ID); !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected gorm.ErrRecordNotFound on second revoke, got %v", err)
	}
}

package database

import (
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

var (
	ErrTenantInviteExpired     = errors.New("tenant invite expired")
	ErrTenantInviteUnavailable = errors.New("tenant invite unavailable")
)

type TenantInviteInfo struct {
	ID          string `json:"id"`
	TenantID    string `json:"tenant_id"`
	TenantName  string `json:"tenant_name,omitempty"`
	TenantSlug  string `json:"tenant_slug,omitempty"`
	Token       string `json:"token"`
	Role        string `json:"role"`
	InviterUUID string `json:"inviter_uuid,omitempty"`
	AcceptedBy  string `json:"accepted_by,omitempty"`
	ExpiresAt   string `json:"expires_at,omitempty"`
	AcceptedAt  string `json:"accepted_at,omitempty"`
	RevokedAt   string `json:"revoked_at,omitempty"`
	CreatedAt   string `json:"created_at,omitempty"`
}

func ListTenantInvites(tenantID string) ([]TenantInviteInfo, error) {
	db := dbcore.GetDBInstance()
	return listTenantInvitesWithDB(db, tenantID)
}

func listTenantInvitesWithDB(db *gorm.DB, tenantID string) ([]TenantInviteInfo, error) {
	type inviteRow struct {
		ID          string
		TenantID    string
		TenantName  string
		TenantSlug  string
		Token       string
		Role        string
		InviterUUID string
		AcceptedBy  string
		ExpiresAt   *models.LocalTime
		AcceptedAt  *models.LocalTime
		RevokedAt   *models.LocalTime
		CreatedAt   models.LocalTime
	}

	var rows []inviteRow
	query := db.Table("tenant_invites").
		Select("tenant_invites.id, tenant_invites.tenant_id, tenants.name AS tenant_name, tenants.slug AS tenant_slug, tenant_invites.token, tenant_invites.role, tenant_invites.inviter_uuid, tenant_invites.accepted_by, tenant_invites.expires_at, tenant_invites.accepted_at, tenant_invites.revoked_at, tenant_invites.created_at").
		Joins("JOIN tenants ON tenants.id = tenant_invites.tenant_id").
		Where("tenant_invites.tenant_id = ?", tenantID).
		Where("tenant_invites.accepted_at IS NULL AND tenant_invites.revoked_at IS NULL").
		Order("tenant_invites.created_at DESC")
	if err := query.Scan(&rows).Error; err != nil {
		return nil, err
	}

	result := make([]TenantInviteInfo, 0, len(rows))
	for _, row := range rows {
		result = append(result, TenantInviteInfo{
			ID:          row.ID,
			TenantID:    row.TenantID,
			TenantName:  row.TenantName,
			TenantSlug:  row.TenantSlug,
			Token:       row.Token,
			Role:        NormalizeTenantRole(row.Role),
			InviterUUID: row.InviterUUID,
			AcceptedBy:  row.AcceptedBy,
			ExpiresAt:   formatLocalTimePtr(row.ExpiresAt),
			AcceptedAt:  formatLocalTimePtr(row.AcceptedAt),
			RevokedAt:   formatLocalTimePtr(row.RevokedAt),
			CreatedAt:   row.CreatedAt.ToTime().Format(time.RFC3339),
		})
	}
	return result, nil
}

func CreateTenantInvite(tenantID, inviterUUID, role string, expiresAt *time.Time) (*models.TenantInvite, error) {
	db := dbcore.GetDBInstance()
	return createTenantInviteWithDB(db, tenantID, inviterUUID, role, expiresAt)
}

func createTenantInviteWithDB(db *gorm.DB, tenantID, inviterUUID, role string, expiresAt *time.Time) (*models.TenantInvite, error) {
	tenantID = strings.TrimSpace(tenantID)
	inviterUUID = strings.TrimSpace(inviterUUID)
	if tenantID == "" || inviterUUID == "" {
		return nil, gorm.ErrRecordNotFound
	}

	if _, err := getTenantByIDWithDB(db, tenantID); err != nil {
		return nil, err
	}

	invite := &models.TenantInvite{
		ID:          uuid.NewString(),
		TenantID:    tenantID,
		Token:       generateTenantInviteToken(),
		InviterUUID: inviterUUID,
		Role:        NormalizeTenantRole(role),
	}
	if expiresAt != nil {
		value := models.FromTime(expiresAt.UTC())
		invite.ExpiresAt = &value
	}

	if err := db.Create(invite).Error; err != nil {
		return nil, err
	}
	return invite, nil
}

func RevokeTenantInvite(tenantID, inviteID string) error {
	db := dbcore.GetDBInstance()
	return revokeTenantInviteWithDB(db, tenantID, inviteID)
}

func revokeTenantInviteWithDB(db *gorm.DB, tenantID, inviteID string) error {
	result := db.Where("tenant_id = ? AND id = ?", tenantID, inviteID).Delete(&models.TenantInvite{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return nil
}

func AcceptTenantInvite(token, userUUID string) (*AccessibleTenant, error) {
	db := dbcore.GetDBInstance()
	return acceptTenantInviteWithDB(db, token, userUUID)
}

func acceptTenantInviteWithDB(db *gorm.DB, token, userUUID string) (*AccessibleTenant, error) {
	token = strings.TrimSpace(token)
	userUUID = strings.TrimSpace(userUUID)
	if token == "" || userUUID == "" {
		return nil, gorm.ErrRecordNotFound
	}

	var acceptedTenant *AccessibleTenant
	err := db.Transaction(func(tx *gorm.DB) error {
		invite, err := getActiveTenantInviteByTokenWithDB(tx, token)
		if err != nil {
			return err
		}
		if invite.ExpiresAt != nil && time.Now().After(invite.ExpiresAt.ToTime()) {
			return ErrTenantInviteExpired
		}

		if err := ensureTenantMemberTx(tx, invite.TenantID, userUUID, invite.Role); err != nil {
			return err
		}

		now := models.FromTime(time.Now())
		if err := tx.Model(&models.TenantInvite{}).
			Where("id = ?", invite.ID).
			Updates(map[string]any{
				"accepted_at": now,
				"accepted_by": userUUID,
			}).Error; err != nil {
			return err
		}

		tenants, err := listAccessibleTenantsByUserWithDB(tx, userUUID, "")
		if err != nil {
			return err
		}
		for _, item := range tenants {
			if item.ID == invite.TenantID {
				tenant := item
				acceptedTenant = &tenant
				break
			}
		}
		if acceptedTenant == nil {
			return gorm.ErrRecordNotFound
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return acceptedTenant, nil
}

func GetTenantInviteByToken(token string) (*TenantInviteInfo, error) {
	db := dbcore.GetDBInstance()
	return getTenantInviteByTokenWithDB(db, token)
}

func getTenantInviteByTokenWithDB(db *gorm.DB, token string) (*TenantInviteInfo, error) {
	invite, err := getActiveTenantInviteByTokenWithDB(db, token)
	if err != nil {
		return nil, err
	}
	tenant, err := getTenantByIDWithDB(db, invite.TenantID)
	if err != nil {
		return nil, err
	}

	info := &TenantInviteInfo{
		ID:          invite.ID,
		TenantID:    invite.TenantID,
		TenantName:  tenant.Name,
		TenantSlug:  tenant.Slug,
		Token:       invite.Token,
		Role:        NormalizeTenantRole(invite.Role),
		InviterUUID: invite.InviterUUID,
		AcceptedBy:  invite.AcceptedBy,
		ExpiresAt:   formatLocalTimePtr(invite.ExpiresAt),
		AcceptedAt:  formatLocalTimePtr(invite.AcceptedAt),
		RevokedAt:   formatLocalTimePtr(invite.RevokedAt),
		CreatedAt:   invite.CreatedAt.ToTime().Format(time.RFC3339),
	}
	if invite.ExpiresAt != nil && time.Now().After(invite.ExpiresAt.ToTime()) {
		return nil, ErrTenantInviteExpired
	}
	return info, nil
}

func getActiveTenantInviteByTokenWithDB(db *gorm.DB, token string) (*models.TenantInvite, error) {
	var invite models.TenantInvite
	if err := db.Where("token = ?", strings.TrimSpace(token)).First(&invite).Error; err != nil {
		return nil, err
	}
	if invite.AcceptedAt != nil || invite.RevokedAt != nil {
		return nil, ErrTenantInviteUnavailable
	}
	return &invite, nil
}

func generateTenantInviteToken() string {
	return strings.ReplaceAll(uuid.NewString(), "-", "") + strings.ReplaceAll(uuid.NewString(), "-", "")
}

func formatLocalTimePtr(value *models.LocalTime) string {
	if value == nil {
		return ""
	}
	return value.ToTime().Format(time.RFC3339)
}

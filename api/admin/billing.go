package admin

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/komari-monitor/komari/api"
	"github.com/komari-monitor/komari/config"
	"github.com/komari-monitor/komari/database/accounts"
	"github.com/komari-monitor/komari/database/billing"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

type createBillingOrderRequest struct {
	PlanID          uint `json:"plan_id" binding:"required"`
	PaymentMethodID uint `json:"payment_method_id" binding:"required"`
}

type paidBillingOrderRequest struct {
	PaymentReference string `json:"payment_reference"`
	AdminNote        string `json:"admin_note"`
}

type cancelBillingOrderRequest struct {
	AdminNote string `json:"admin_note"`
}

func GetBillingCatalog(c *gin.Context) {
	userUUID, ok := currentUserUUID(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "User context is required")
		return
	}

	plans, err := billing.ListPlans(false)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list billing plans: "+err.Error())
		return
	}
	methods, err := billing.ListPaymentMethods(false)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list payment methods: "+err.Error())
		return
	}
	policy, err := config.GetUserPolicy(userUUID)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to load user policy: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{
		"plans":              plans,
		"payment_methods":    methods,
		"policy":             policy,
		"available_features": config.UserAvailableFeatures(),
	})
}

func CreateBillingOrder(c *gin.Context) {
	userUUID, ok := currentUserUUID(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "User context is required")
		return
	}

	var req createBillingOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}

	order, err := billing.CreateOrder(userUUID, req.PlanID, req.PaymentMethodID)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, gorm.ErrRecordNotFound) {
			status = http.StatusNotFound
		}
		api.RespondError(c, status, "Failed to create billing order: "+err.Error())
		return
	}
	api.RespondSuccess(c, buildBillingOrderItem(order))
}

func GetMyBillingOrders(c *gin.Context) {
	userUUID, ok := currentUserUUID(c)
	if !ok {
		api.RespondError(c, http.StatusForbidden, "User context is required")
		return
	}
	orders, err := billing.ListOrders(billing.OrderFilter{UserUUID: userUUID, Limit: 100})
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list billing orders: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{"items": buildBillingOrderItems(orders)})
}

func ListBillingPlans(c *gin.Context) {
	plans, err := billing.ListPlans(true)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list billing plans: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{
		"items":              plans,
		"available_features": config.UserAvailableFeatures(),
	})
}

func SaveBillingPlan(c *gin.Context) {
	var req models.BillingPlan
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	plan, err := billing.SavePlan(req)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Failed to save billing plan: "+err.Error())
		return
	}
	api.RespondSuccess(c, plan)
}

func ArchiveBillingPlan(c *gin.Context) {
	id, ok := billingIDParam(c)
	if !ok {
		return
	}
	if err := billing.ArchivePlan(id); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Failed to archive billing plan: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{"id": id})
}

func ListPaymentMethods(c *gin.Context) {
	methods, err := billing.ListPaymentMethods(true)
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list payment methods: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{"items": methods})
}

func SavePaymentMethod(c *gin.Context) {
	var req models.PaymentMethod
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	method, err := billing.SavePaymentMethod(req)
	if err != nil {
		api.RespondError(c, http.StatusBadRequest, "Failed to save payment method: "+err.Error())
		return
	}
	api.RespondSuccess(c, method)
}

func DisablePaymentMethod(c *gin.Context) {
	id, ok := billingIDParam(c)
	if !ok {
		return
	}
	if err := billing.DisablePaymentMethod(id); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Failed to disable payment method: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{"id": id})
}

func ListBillingOrders(c *gin.Context) {
	orders, err := billing.ListOrders(billing.OrderFilter{
		Status: strings.TrimSpace(c.Query("status")),
		Limit:  200,
	})
	if err != nil {
		api.RespondError(c, http.StatusInternalServerError, "Failed to list billing orders: "+err.Error())
		return
	}
	api.RespondSuccess(c, gin.H{"items": buildBillingOrderItems(orders)})
}

func MarkBillingOrderPaid(c *gin.Context) {
	id, ok := billingIDParam(c)
	if !ok {
		return
	}
	var req paidBillingOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	order, err := billing.MarkOrderPaid(id, req.PaymentReference, req.AdminNote)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, billing.ErrOrderClosed) {
			status = http.StatusConflict
		}
		api.RespondError(c, status, "Failed to mark billing order paid: "+err.Error())
		return
	}
	api.RespondSuccess(c, buildBillingOrderItem(order))
}

func CancelBillingOrder(c *gin.Context) {
	id, ok := billingIDParam(c)
	if !ok {
		return
	}
	var req cancelBillingOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		api.RespondError(c, http.StatusBadRequest, "Invalid request: "+err.Error())
		return
	}
	order, err := billing.CancelOrder(id, req.AdminNote)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, billing.ErrOrderClosed) {
			status = http.StatusConflict
		}
		api.RespondError(c, status, "Failed to cancel billing order: "+err.Error())
		return
	}
	api.RespondSuccess(c, buildBillingOrderItem(order))
}

func billingIDParam(c *gin.Context) (uint, bool) {
	raw := strings.TrimSpace(c.Param("id"))
	id, err := strconv.ParseUint(raw, 10, 64)
	if err != nil || id == 0 {
		api.RespondError(c, http.StatusBadRequest, "Invalid billing id")
		return 0, false
	}
	return uint(id), true
}

func buildBillingOrderItems(orders []models.BillingOrder) []gin.H {
	items := make([]gin.H, 0, len(orders))
	for _, order := range orders {
		items = append(items, buildBillingOrderItem(order))
	}
	return items
}

func buildBillingOrderItem(order models.BillingOrder) gin.H {
	username := ""
	if user, err := accounts.GetUserByUUID(order.UserUUID); err == nil {
		username = user.Username
	}
	return gin.H{
		"id":                order.ID,
		"order_no":          order.OrderNo,
		"user_uuid":         order.UserUUID,
		"username":          username,
		"plan_id":           order.PlanID,
		"payment_method_id": order.PaymentMethodID,
		"status":            order.Status,
		"plan_code":         order.PlanCode,
		"plan_name":         order.PlanName,
		"amount_cents":      order.AmountCents,
		"currency":          order.Currency,
		"duration_days":     order.DurationDays,
		"server_quota":      order.ServerQuota,
		"allowed_features":  order.AllowedFeatures,
		"payment_code":      order.PaymentCode,
		"payment_name":      order.PaymentName,
		"payment_reference": order.PaymentReference,
		"admin_note":        order.AdminNote,
		"paid_at":           order.PaidAt,
		"fulfilled_at":      order.FulfilledAt,
		"created_at":        order.CreatedAt,
		"updated_at":        order.UpdatedAt,
	}
}

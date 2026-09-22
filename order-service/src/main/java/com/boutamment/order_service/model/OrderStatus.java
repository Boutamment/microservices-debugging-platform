package com.boutamment.order_service.model;

public enum OrderStatus {
    PENDING, CONFIRMED, PAYMENT_PROCESSING, PAYMENT_FAILED,
    INVENTORY_RESERVED, COMPLETED, CANCELLED
}

package com.tushar.sentinel.repository.alert;

/** Alert workflow state; alerts are never deleted, only dispositioned. */
public enum AlertStatus {
    OPEN, IN_REVIEW, CLOSED, ESCALATED
}

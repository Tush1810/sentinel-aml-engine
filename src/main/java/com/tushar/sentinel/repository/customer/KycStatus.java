package com.tushar.sentinel.repository.customer;

/** KYC verification state; anything but VERIFIED raises a customer's risk. */
public enum KycStatus {
    VERIFIED, PENDING, REJECTED
}

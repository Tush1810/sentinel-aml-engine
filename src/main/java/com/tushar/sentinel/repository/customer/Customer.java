package com.tushar.sentinel.repository.customer;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

/** Bank customer and their KYC profile. Owns one or more {@code Account}s. */
@Entity
@Table(name = "customer")
@Getter
@Setter
@NoArgsConstructor
public class Customer {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Business identifier from the source system, e.g. CUST_00001. */
    @Column(name = "customer_ref", nullable = false, unique = true, length = 32)
    private String customerRef;

    @Column(name = "first_name", nullable = false, length = 100)
    private String firstName;

    @Column(name = "last_name", nullable = false, length = 100)
    private String lastName;

    @Column(length = 16)
    private String gender;

    @Column(name = "date_of_birth")
    private LocalDate dateOfBirth;

    @Column(length = 160)
    private String email;

    @Column(name = "phone_number", length = 32)
    private String phoneNumber;

    @Column(length = 100)
    private String city;

    @Column(length = 100)
    private String state;

    @Column(length = 2)
    private String country;

    @Column(name = "postal_code", length = 16)
    private String postalCode;

    @Column(length = 100)
    private String occupation;

    @Column(name = "annual_income", precision = 18, scale = 2)
    private BigDecimal annualIncome;

    @Column(name = "marital_status", length = 32)
    private String maritalStatus;

    @Column(name = "education_level", length = 64)
    private String educationLevel;

    @Column(name = "employment_status", length = 32)
    private String employmentStatus;

    @Column(name = "customer_since")
    private LocalDate customerSince;

    @Column(name = "customer_segment", length = 32)
    private String customerSegment;

    @Enumerated(EnumType.STRING)
    @Column(name = "kyc_status", nullable = false, length = 16)
    private KycStatus kycStatus;

    @Enumerated(EnumType.STRING)
    @Column(name = "risk_rating", nullable = false, length = 16)
    private RiskRating riskRating;

    /** PEP status mandates enhanced due diligence and lifts the alert risk score. */
    @Column(name = "politically_exposed", nullable = false)
    private boolean politicallyExposed;

    @Column(name = "preferred_channel", length = 48)
    private String preferredChannel;

    @Column(name = "email_verified", nullable = false)
    private boolean emailVerified;

    @Column(name = "phone_verified", nullable = false)
    private boolean phoneVerified;

    @Column(name = "num_complaints_last_year", nullable = false)
    private int numComplaintsLastYear;

    @CreationTimestamp
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;
}

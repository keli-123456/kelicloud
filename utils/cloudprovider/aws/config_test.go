package aws

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCredentialRecordSetCheckResultStoresQuota(t *testing.T) {
	credential := &CredentialRecord{
		ID:              "cred-1",
		Name:            "primary",
		AccessKeyID:     "AKIA_TEST",
		SecretAccessKey: "secret",
	}

	credential.SetCheckResult(time.Unix(1710000000, 0), &Identity{
		AccountID: "123456789012",
		ARN:       "arn:aws:iam::123456789012:user/test",
		UserID:    "AIDATEST",
	}, &EC2QuotaSummary{
		Region:                           "us-west-2",
		MaxStandardVCPUs:                 32,
		MaxInstances:                     20,
		MaxElasticIPs:                    5,
		VPCMaxSecurityGroupsPerInterface: 5,
		InstanceStandardVCPUs:            4,
		ReservedStandardVCPUs:            4,
		RunningStandardVCPUs:             8,
		RunningInstances:                 3,
		TotalInstances:                   4,
		AllocatedElasticIPs:              2,
		AssociatedElasticIPs:             1,
	}, nil, nil)

	require.Equal(t, CredentialStatusHealthy, credential.LastStatus)
	require.Empty(t, credential.LastError)
	require.Equal(t, "123456789012", credential.AccountID)
	require.NotNil(t, credential.EC2Quota)
	require.Equal(t, "us-west-2", credential.EC2Quota.Region)
	require.Equal(t, 32, credential.EC2Quota.MaxStandardVCPUs)
	require.Equal(t, 20, credential.EC2Quota.MaxInstances)
	require.Equal(t, 5, credential.EC2Quota.MaxElasticIPs)
	require.Equal(t, 5, credential.EC2Quota.VPCMaxSecurityGroupsPerInterface)
	require.Equal(t, 4, credential.EC2Quota.InstanceStandardVCPUs)
	require.Equal(t, 4, credential.EC2Quota.ReservedStandardVCPUs)
	require.Equal(t, 8, credential.EC2Quota.RunningStandardVCPUs)
	require.Equal(t, 3, credential.EC2Quota.RunningInstances)
	require.Equal(t, 4, credential.EC2Quota.TotalInstances)
	require.Equal(t, 2, credential.EC2Quota.AllocatedElasticIPs)
	require.Equal(t, 1, credential.EC2Quota.AssociatedElasticIPs)
	require.Empty(t, credential.EC2QuotaError)
}

func TestNormalizeEC2QuotaSummaryKeepsUsageOnlyData(t *testing.T) {
	summary := normalizeEC2QuotaSummary(&EC2QuotaSummary{
		Region:                "us-east-1",
		InstanceStandardVCPUs: 4,
		ReservedStandardVCPUs: 4,
		RunningStandardVCPUs:  8,
		RunningInstances:      2,
		AllocatedElasticIPs:   1,
	})

	require.NotNil(t, summary)
	require.Equal(t, "us-east-1", summary.Region)
	require.Equal(t, 4, summary.InstanceStandardVCPUs)
	require.Equal(t, 4, summary.ReservedStandardVCPUs)
	require.Equal(t, 8, summary.RunningStandardVCPUs)
	require.Equal(t, 2, summary.RunningInstances)
	require.Equal(t, 1, summary.AllocatedElasticIPs)
}

func TestCredentialRecordSetCheckResultKeepsHealthyWhenQuotaLookupFails(t *testing.T) {
	credential := &CredentialRecord{
		ID:              "cred-1",
		Name:            "primary",
		AccessKeyID:     "AKIA_TEST",
		SecretAccessKey: "secret",
	}

	quotaErr := errors.New("quota lookup denied")
	credential.SetCheckResult(time.Unix(1710000000, 0), &Identity{
		AccountID: "123456789012",
	}, nil, quotaErr, nil)

	require.Equal(t, CredentialStatusHealthy, credential.LastStatus)
	require.Empty(t, credential.LastError)
	require.Equal(t, "123456789012", credential.AccountID)
	require.Nil(t, credential.EC2Quota)
	require.Equal(t, quotaErr.Error(), credential.EC2QuotaError)
}

func TestCredentialRecordSetCheckResultClearsQuotaOnIdentityError(t *testing.T) {
	credential := &CredentialRecord{
		ID:              "cred-1",
		Name:            "primary",
		AccessKeyID:     "AKIA_TEST",
		SecretAccessKey: "secret",
		EC2Quota: &EC2QuotaSummary{
			Region:       "us-east-1",
			MaxInstances: 20,
		},
		EC2QuotaError: "old quota error",
	}

	identityErr := errors.New("invalid credential")
	credential.SetCheckResult(time.Unix(1710000000, 0), nil, nil, nil, identityErr)

	require.Equal(t, CredentialStatusError, credential.LastStatus)
	require.Equal(t, identityErr.Error(), credential.LastError)
	require.Empty(t, credential.AccountID)
	require.Nil(t, credential.EC2Quota)
	require.Empty(t, credential.EC2QuotaError)
}

func TestAdditionUpsertCredentialsGeneratesUniqueDefaultNames(t *testing.T) {
	addition := &Addition{
		Credentials: []CredentialRecord{
			{
				ID:              "cred-1",
				Name:            "Credential 1",
				AccessKeyID:     "AKIAEXISTING",
				SecretAccessKey: "secret-1",
				DefaultRegion:   "us-east-1",
			},
		},
	}

	count := addition.UpsertCredentials([]CredentialImport{
		{
			AccessKeyID:     "AKIASECOND",
			SecretAccessKey: "secret-2",
			DefaultRegion:   "us-east-1",
		},
		{
			AccessKeyID:     "AKIATHIRD",
			SecretAccessKey: "secret-3",
			DefaultRegion:   "us-west-2",
		},
	})

	require.Equal(t, 2, count)
	require.Len(t, addition.Credentials, 3)
	require.Equal(t, "Credential 1", addition.Credentials[0].Name)
	require.Equal(t, "Credential 2", addition.Credentials[1].Name)
	require.Equal(t, "Credential 3", addition.Credentials[2].Name)
}

func TestAdditionRemoveCredentialClearsLegacyCredentialWhenLastEntryDeleted(t *testing.T) {
	addition := &Addition{
		AccessKeyID:     "AKIALEGACY",
		SecretAccessKey: "legacy-secret",
		SessionToken:    "legacy-session",
		DefaultRegion:   "us-east-1",
	}

	addition.Normalize()
	require.Len(t, addition.Credentials, 1)

	credentialID := addition.Credentials[0].ID
	require.True(t, addition.RemoveCredential(credentialID))
	require.Empty(t, addition.Credentials)
	require.Empty(t, addition.ActiveCredentialID)
	require.Empty(t, addition.AccessKeyID)
	require.Empty(t, addition.SecretAccessKey)
	require.Empty(t, addition.SessionToken)

	addition.Normalize()
	require.Empty(t, addition.Credentials)
}

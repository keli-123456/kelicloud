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
		MaxInstances:                     20,
		MaxElasticIPs:                    5,
		VPCMaxSecurityGroupsPerInterface: 5,
	}, nil, nil)

	require.Equal(t, CredentialStatusHealthy, credential.LastStatus)
	require.Empty(t, credential.LastError)
	require.Equal(t, "123456789012", credential.AccountID)
	require.NotNil(t, credential.EC2Quota)
	require.Equal(t, "us-west-2", credential.EC2Quota.Region)
	require.Equal(t, 20, credential.EC2Quota.MaxInstances)
	require.Equal(t, 5, credential.EC2Quota.MaxElasticIPs)
	require.Equal(t, 5, credential.EC2Quota.VPCMaxSecurityGroupsPerInterface)
	require.Empty(t, credential.EC2QuotaError)
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

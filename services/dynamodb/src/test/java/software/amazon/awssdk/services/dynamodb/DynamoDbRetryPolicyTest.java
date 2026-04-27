package software.amazon.awssdk.services.dynamodb;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.core.client.config.HedgingConfig;
import software.amazon.awssdk.core.client.config.SdkClientConfiguration;
import software.amazon.awssdk.core.client.config.SdkClientOption;
import software.amazon.awssdk.core.internal.retry.SdkDefaultRetryStrategy;
import software.amazon.awssdk.core.retry.RetryMode;
import software.amazon.awssdk.profiles.ProfileFile;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.testutils.EnvironmentVariableHelper;
import software.amazon.awssdk.utils.StringInputStream;

class DynamoDbRetryPolicyTest {

    private EnvironmentVariableHelper environmentVariableHelper;

    @BeforeEach
    public void setup() {
        environmentVariableHelper = new EnvironmentVariableHelper();
    }

    @AfterEach
    public void reset() {
        environmentVariableHelper.reset();
    }

    @Test
    void test_numRetries_with_standardRetryPolicy() {
        environmentVariableHelper.set(SdkSystemSetting.AWS_RETRY_MODE.environmentVariable(), "standard");
        SdkClientConfiguration sdkClientConfiguration = SdkClientConfiguration.builder().build();
        RetryStrategy retryStrategy = DynamoDbRetryPolicy.resolveRetryStrategy(sdkClientConfiguration);
        assertThat(retryStrategy.maxAttempts()).isEqualTo(9);
    }

    @Test
    void test_numRetries_with_legacyRetryPolicy() {
        environmentVariableHelper.set(SdkSystemSetting.AWS_RETRY_MODE.environmentVariable(), "legacy");
        SdkClientConfiguration sdkClientConfiguration = SdkClientConfiguration.builder().build();
        RetryStrategy retryStrategy = DynamoDbRetryPolicy.resolveRetryStrategy(sdkClientConfiguration);
        assertThat(retryStrategy.maxAttempts()).isEqualTo(9);
    }

    @Test
    void resolve_retryModeSetInEnv_doesNotCallSupplier() {
        environmentVariableHelper.set(SdkSystemSetting.AWS_RETRY_MODE.environmentVariable(), "standard");
        SdkClientConfiguration sdkClientConfiguration = SdkClientConfiguration.builder().build();
        RetryStrategy retryStrategy = DynamoDbRetryPolicy.resolveRetryStrategy(sdkClientConfiguration);
        RetryMode retryMode = SdkDefaultRetryStrategy.retryMode(retryStrategy);
        assertThat(retryMode).isEqualTo(RetryMode.STANDARD);
    }

    @Test
    void resolve_retryModeSetWithEnvAndSupplier_resolvesFromEnv() {
        environmentVariableHelper.set(SdkSystemSetting.AWS_RETRY_MODE.environmentVariable(), "standard");
        ProfileFile profileFile = ProfileFile.builder()
                                             .content(new StringInputStream("[profile default]\n"
                                                                            + "retry_mode = adaptive"))
                                             .type(ProfileFile.Type.CONFIGURATION)
                                             .build();
        SdkClientConfiguration sdkClientConfiguration = SdkClientConfiguration
            .builder()
            .option(SdkClientOption.PROFILE_FILE_SUPPLIER, () -> profileFile)
            .option(SdkClientOption.PROFILE_NAME, "default")
            .build();
        RetryStrategy retryStrategy = DynamoDbRetryPolicy.resolveRetryStrategy(sdkClientConfiguration);
        RetryMode retryMode = SdkDefaultRetryStrategy.retryMode(retryStrategy);

        assertThat(retryMode).isEqualTo(RetryMode.STANDARD);
    }

    @Test
    void resolve_retryModeSetWithSupplier_resolvesFromSupplier() {
        ProfileFile profileFile = ProfileFile.builder()
                                             .content(new StringInputStream("[profile default]\n"
                                                                            + "retry_mode = adaptive"))
                                             .type(ProfileFile.Type.CONFIGURATION)
                                             .build();
        SdkClientConfiguration sdkClientConfiguration = SdkClientConfiguration
            .builder()
            .option(SdkClientOption.PROFILE_FILE_SUPPLIER, () -> profileFile)
            .option(SdkClientOption.PROFILE_NAME, "default")
            .build();
        RetryStrategy retryStrategy = DynamoDbRetryPolicy.resolveRetryStrategy(sdkClientConfiguration);
        RetryMode retryMode = SdkDefaultRetryStrategy.retryMode(retryStrategy);

        assertThat(retryMode).isEqualTo(RetryMode.ADAPTIVE_V2);
    }

    @Test
    void resolve_retryModeSetWithSdkClientOption_resolvesFromSdkClientOption() {
        ProfileFile profileFile = ProfileFile.builder()
                                             .content(new StringInputStream("[profile default]\n"))
                                             .type(ProfileFile.Type.CONFIGURATION)
                                             .build();
        SdkClientConfiguration sdkClientConfiguration = SdkClientConfiguration
            .builder()
            .option(SdkClientOption.PROFILE_FILE_SUPPLIER, () -> profileFile)
            .option(SdkClientOption.PROFILE_NAME, "default")
            .option(SdkClientOption.DEFAULT_RETRY_MODE, RetryMode.STANDARD)
            .build();
        RetryStrategy retryStrategy = DynamoDbRetryPolicy.resolveRetryStrategy(sdkClientConfiguration);
        RetryMode retryMode = SdkDefaultRetryStrategy.retryMode(retryStrategy);

        assertThat(retryMode).isEqualTo(RetryMode.STANDARD);
    }

    @Test
    void resolve_retryModeNotSetWithEnvNorSupplier_resolvesFromSdkDefault() {
        ProfileFile profileFile = ProfileFile.builder()
                                             .content(new StringInputStream("[profile default]\n"))
                                             .type(ProfileFile.Type.CONFIGURATION)
                                             .build();
        SdkClientConfiguration sdkClientConfiguration = SdkClientConfiguration
            .builder()
            .option(SdkClientOption.PROFILE_FILE_SUPPLIER, () -> profileFile)
            .option(SdkClientOption.PROFILE_NAME, "default")
            .build();
        RetryStrategy retryStrategy = DynamoDbRetryPolicy.resolveRetryStrategy(sdkClientConfiguration);
        RetryMode retryMode = SdkDefaultRetryStrategy.retryMode(retryStrategy);

        assertThat(retryMode).isEqualTo(RetryMode.LEGACY);
    }

    @Test
    void defaultHedgingConfig_hasExpectedDefaults() {
        HedgingConfig config =
            DynamoDbRetryPolicy.defaultHedgingConfig();
        HedgingConfig.OperationHedgingPolicy defaultPolicy = config.defaultPolicy();
        HedgingConfig.OperationHedgingPolicy getItemPolicy = config.policyForOperation("GetItem");

        assertThat(config.enabled()).isFalse();
        assertThat(defaultPolicy.maxHedgedAttempts()).isEqualTo(3);
        assertThat(((HedgingConfig.FixedDelayConfig) defaultPolicy.delayConfig()).baseDelay())
            .isEqualTo(Duration.ofMillis(10));
        assertThat(config.policyPerOperation()).containsOnlyKeys("GetItem");
        assertThat(getItemPolicy.maxHedgedAttempts()).isEqualTo(3);
        assertThat(((HedgingConfig.FixedDelayConfig) getItemPolicy.delayConfig()).baseDelay())
            .isEqualTo(Duration.ofMillis(8));
        assertThat(config.hedgeableOperations()).containsExactlyInAnyOrder("GetItem", "Query", "Scan");
        assertThat(config.shouldHedge("GetItem")).isFalse();
        assertThat(config.shouldHedge("Query")).isFalse();
        assertThat(config.shouldHedge("Scan")).isFalse();
        assertThat(config.shouldHedge("PutItem")).isFalse();
        assertThat(config.shouldHedge("BatchGetItem")).isFalse();
    }

}

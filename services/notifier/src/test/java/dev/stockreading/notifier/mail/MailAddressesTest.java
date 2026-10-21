package dev.stockreading.notifier.mail;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class MailAddressesTest {

    @ParameterizedTest
    @CsvSource({
            "alice@example.com,   a****@example.com",
            "x@example.com,       x****@example.com",
            "longname@example.org, l****@example.org"
    })
    void keepsOnlyTheFirstLetterAndTheDomain(String address, String expected) {
        assertThat(MailAddresses.mask(address)).isEqualTo(expected);
    }

    @ParameterizedTest
    @NullSource
    @ValueSource(strings = {"", "no-at-sign", "@example.com", "alice@"})
    void anythingThatIsNotAnAddressIsFullyHidden(String value) {
        assertThat(MailAddresses.mask(value)).isEqualTo("***");
    }

    @org.junit.jupiter.api.Test
    void redactHidesEveryAddressInFreeText() {
        String text = "554 Access denied for alice@example.com; also bob.smith@mail.example.org was copied";

        assertThat(MailAddresses.redact(text))
                .doesNotContain("alice@example.com").doesNotContain("bob.smith@mail.example.org")
                .contains("a****@example.com").contains("b****@mail.example.org");
    }

    @org.junit.jupiter.api.Test
    void redactHidesTheAwsAccountNumberInArns() {
        String arn = "User 'arn:aws:iam::123456789012:user/ses-smtp-user' cannot use 'arn:aws:ses:ap-southeast-2:123456789012:identity/alice@example.com'";

        assertThat(MailAddresses.redact(arn))
                .doesNotContain("123456789012")
                .contains("arn:aws:iam::************:user/ses-smtp-user")
                .contains("arn:aws:ses:ap-southeast-2:************:identity/a****@example.com");
    }

    @org.junit.jupiter.api.Test
    void redactLeavesOrdinaryTextAndNullAlone() {
        assertThat(MailAddresses.redact("535 Authentication failed")).isEqualTo("535 Authentication failed");
        assertThat(MailAddresses.redact(null)).isNull();
    }
}

package dev.stockreading.notifier.mail;

/** 이메일 주소는 개인정보다: 로그에 전체가 나오면 안 된다. */
public final class MailAddresses {

    private static final java.util.regex.Pattern EMAIL =
            java.util.regex.Pattern.compile("[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}");
    // "arn:aws:ses:us-east-1:123456789012:identity/..." -> 12자리 계정 번호를 가린다.
    private static final java.util.regex.Pattern ARN_ACCOUNT =
            java.util.regex.Pattern.compile("(arn:aws[a-z-]*:[a-z0-9-]*:[a-z0-9-]*:)\\d{12}");

    private MailAddresses() {
    }

    /** 자유 텍스트(예: SMTP 서버의 오류 메시지) 안의 모든 이메일 주소와 AWS 계정 번호를 가린다. */
    public static String redact(String text) {
        if (text == null) {
            return null;
        }
        String withoutAccounts = ARN_ACCOUNT.matcher(text).replaceAll("$1************");
        return EMAIL.matcher(withoutAccounts).replaceAll(match -> java.util.regex.Matcher.quoteReplacement(mask(match.group())));
    }

    /** "alice@example.com" -> "a****@example.com". 주소가 아니면 "***"로 바꾼다. */
    public static String mask(String address) {
        if (address == null) {
            return "***";
        }
        int at = address.indexOf('@');
        if (at < 1 || at == address.length() - 1) {
            return "***";
        }
        return address.charAt(0) + "****" + address.substring(at);
    }
}

import pytest

from briefing.sources import parse_close_rates, parse_filings

KST_TEXT = "2026-10-20T16:00:00+09:00"


def test_close_rates_are_parsed_with_their_times():
    rates = parse_close_rates([{"ticker": "TESTA", "date": "20261019", "closeAt": KST_TEXT, "rate": "1.20",
                                "fetchedAt": KST_TEXT}])
    assert rates[0].ticker == "TESTA" and rates[0].rate == "1.20"
    assert rates[0].close_at.utcoffset() is not None and rates[0].fetched_at == rates[0].close_at


def test_filings_are_parsed_with_tuples():
    filings = parse_filings([{"accessionNumber": "0000000000-26-000001", "tickers": ["TESTA"], "form": "8-K",
                              "items": ["2.02"], "acceptedAt": KST_TEXT, "url": "https://example.com/f"}])
    assert filings[0].accession_number == "0000000000-26-000001"
    assert filings[0].tickers == ("TESTA",) and filings[0].items == ("2.02",)


@pytest.mark.parametrize("parse", [parse_close_rates, parse_filings])
def test_missing_field_raises_key_error_for_the_caller_to_wrap(parse):
    with pytest.raises(KeyError):
        parse([{}])

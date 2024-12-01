from pik.rules import (
    FlightRule, AircraftFilter, PeriodFilter, CappedRule, AllRules, FirstRule, 
    SimpleRule, OrFilter, PurposeFilter, InvoicingChargeFilter, TransferTowFilter, 
    SetLedgerYearRule, PositivePriceFilter, NegativePriceFilter, BirthDateFilter, 
    MinimumDurationRule, MemberListFilter
)

from pik.util import Period
from pik.billing import BillingContext
from pik.reader import load_configuration
from pik.processor import process_billing

import datetime as dt
import sys

def make_rules(ctx=BillingContext(), metadata=None):
    # Configuration
    YEAR = 2024
    F_MOTOR_PERIOD = [PeriodFilter(Period(dt.date(YEAR, 1, 28), dt.date(YEAR, 10, 27)))]
    F_GLIDER_SEASON = F_FULL_YEAR = [PeriodFilter(Period.full_year(YEAR))]

    ACCT_PURSI_KEIKKA = 3220
    ACCT_TOW = 3130
    ACCT_1037 = 3150 # Lentotuntitulot jäseniltä
    ACCT_1037_OPEALE = 3150 # Lentotuntitulot jäseniltä
    ACCT_TOWING = 3170 # Muut lentotoiminnan tulot
    ACCT_PURSI_INSTRUCTION = 3470 # Muut tulot koulutustoiminnasta
    ACCT_KALUSTO = 3010
    ACCT_LASKUTUSLISA = 3610 # Hallinnon tulot

    ID_PURSI_CAP_2024 = f"pursi_hintakatto_{YEAR}"
    ID_KALUSTOMAKSU_CAP_2024 = f"kalustomaksu_hintakatto_{YEAR}"

    birth_dates = (metadata or {}).get("birth_dates", {})
    member_ids = (metadata or {}).get("course_members", set())
    
    F_YOUTH = [BirthDateFilter(birth_dates, 25)]
    F_KURSSI = [MemberListFilter(member_ids)]

    F_FK = [AircraftFilter("650")]
    F_FM = [AircraftFilter("787")]
    F_FQ = [AircraftFilter("733")]
    F_FY = [AircraftFilter("883")]
    F_FI = [AircraftFilter("1035")]
    F_DG = [AircraftFilter("952")]
    F_TOW = [AircraftFilter("TOW")]
    F_1037 = [AircraftFilter("1037")]
    F_1037_OPEALE = [AircraftFilter("1037-opeale")]

    F_MOTTI = [OrFilter([F_TOW + F_1037+ F_1037_OPEALE])]
    F_PURTSIKKA = [OrFilter([F_FK + F_FM + F_FQ + F_FY + F_FI + F_DG])]
    F_KAIKKI_KONEET = [OrFilter([F_MOTTI + F_PURTSIKKA])]

    F_LASKUTUSLISA = [InvoicingChargeFilter()]
    F_TRANSFER_TOW = [TransferTowFilter()]

    rules = [
        # OH-TOW
        FirstRule([
            # Nuorisoalennus + siirtohinaus
            MinimumDurationRule(
                FlightRule(122 * 0.75, ACCT_TOWING, 
                          F_TOW + F_MOTOR_PERIOD + F_TRANSFER_TOW + F_YOUTH,
                          "Siirtohinaus, TOW (nuorisoalennus), %(duration)d min"),
                F_MOTTI, 15, "(minimilaskutus 15 min)"),
            
            # Nuorisoalennus
            MinimumDurationRule(
                FlightRule(122 * 0.75, ACCT_TOW,
                          F_TOW + F_MOTOR_PERIOD + F_YOUTH,
                          "Lento, TOW (nuorisoalennus), %(duration)d min"),
                F_MOTTI, 15, "(minimilaskutus 15 min)"),
            
            # Siirtohinaus
            MinimumDurationRule(
                FlightRule(122, ACCT_TOWING,
                          F_TOW + F_MOTOR_PERIOD + F_TRANSFER_TOW,
                          "Siirtohinaus, TOW, %(duration)d min"),
                F_MOTTI, 15, "(minimilaskutus 15 min)"),
            
            # Normaalilento
            MinimumDurationRule(
                FlightRule(122, ACCT_TOW,
                          F_TOW + F_MOTOR_PERIOD,
                          "Lento, TOW, %(duration)d min"),
                F_MOTTI, 15, "(minimilaskutus 15 min)")
        ]),

        # OH-1037
        FirstRule([
            # Nuorisoalennus
            MinimumDurationRule(
                FlightRule(113 * 0.75, ACCT_1037,
                          F_1037 + F_MOTOR_PERIOD + F_YOUTH,
                          "Lento, 1037 (nuorisoalennus), %(duration)d min"),
                F_MOTTI, 15, "(minimilaskutus 15 min)"),
            
            # Normaalilento
            MinimumDurationRule(
                FlightRule(113, ACCT_1037,
                          F_1037 + F_MOTOR_PERIOD,
                          "Lento, 1037, %(duration)d min"),
                F_MOTTI, 15, "(minimilaskutus 15 min)")
        ]),

        # OH-1037 opeale
        FlightRule(65, ACCT_1037_OPEALE, F_1037_OPEALE + F_MOTOR_PERIOD, "Lento (opealennus), %(duration)d min"),

        # Purtsikat
        CappedRule(ID_PURSI_CAP_2024, 1250, ctx,
        AllRules([
            # Purtsikat
            FirstRule([
                FlightRule(18 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FK + F_YOUTH, "Lento (nuorisoalennus), %(aircraft)s, %(duration)d min"),
                FlightRule(18 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FK + F_KURSSI, "Lento (kurssialennus), %(aircraft)s, %(duration)d min"),
                FlightRule(18, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FK)
            ]),
            FirstRule([
                FlightRule(26 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FM + F_YOUTH, "Lento (nuorisoalennus), %(aircraft)s, %(duration)d min"),
                FlightRule(26 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FM + F_KURSSI, "Lento (kurssialennus), %(aircraft)s, %(duration)d min"),
                FlightRule(26, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FM)
            ]),
            FirstRule([
                FlightRule(28 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FQ + F_YOUTH, "Lento (nuorisoalennus), %(aircraft)s, %(duration)d min"),
                FlightRule(28 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FQ + F_KURSSI, "Lento (kurssialennus), %(aircraft)s, %(duration)d min"),
                FlightRule(28, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FQ)
            ]),
            FirstRule([
                FlightRule(29 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FI + F_YOUTH, "Lento (nuorisoalennus), %(aircraft)s, %(duration)d min"),
                FlightRule(29 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FI + F_KURSSI, "Lento (kurssialennus), %(aircraft)s, %(duration)d min"),
                FlightRule(29, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FI)
            ]),
            FirstRule([
                FlightRule(36 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FY + F_YOUTH, "Lento (nuorisoalennus), %(aircraft)s, %(duration)d min"),
                FlightRule(36 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FY + F_KURSSI, "Lento (kurssialennus), %(aircraft)s, %(duration)d min"),
                FlightRule(36, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_FY)
            ]),
            FirstRule([
                FlightRule(44 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_DG + F_YOUTH, "Lento (nuorisoalennus), %(aircraft)s, %(duration)d min"),
                FlightRule(44 * 0.75, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_DG + F_KURSSI, "Lento (kurssialennus), %(aircraft)s, %(duration)d min"),
                FlightRule(44, ACCT_PURSI_KEIKKA, F_GLIDER_SEASON + F_DG)
            ])
        ])),

        # Koululentomaksu
        FlightRule(lambda ev: 6, ACCT_PURSI_INSTRUCTION, F_PURTSIKKA + F_GLIDER_SEASON + [PurposeFilter("KOU")], "Koululentomaksu, %(aircraft)s"),

        # Kalustomaksu
        CappedRule(ID_KALUSTOMAKSU_CAP_2024, 90, ctx,
                   AllRules([FlightRule(10, ACCT_KALUSTO, F_GLIDER_SEASON + F_PURTSIKKA,
                                    "Kalustomaksu, %(aircraft)s, %(duration)d min"),
                            FlightRule(10, ACCT_KALUSTO, F_GLIDER_SEASON + F_MOTTI,
                                    "Kalustomaksu, %(aircraft)s, %(duration)d min")])),

        # Normal simple events
        FirstRule([
            SimpleRule(F_FULL_YEAR + [PositivePriceFilter()]),
            SimpleRule(F_FULL_YEAR + [NegativePriceFilter()])
        ]),

        FlightRule(lambda ev: 2, ACCT_LASKUTUSLISA, F_KAIKKI_KONEET + F_GLIDER_SEASON + F_LASKUTUSLISA, "Laskutuslisä, %(aircraft)s, %(invoicing_comment)s")
    ]
    
    return [SetLedgerYearRule(AllRules(rules), YEAR)]

if __name__ == '__main__':
    if len(sys.argv) < 2 or not (sys.argv[1].endswith('.py') or sys.argv[1].endswith('.json')):
        print("Usage: invoice-flights.py <config.py|config.json>")
        sys.exit(1)

    conf = load_configuration(sys.argv[1])
    process_billing(conf, make_rules)
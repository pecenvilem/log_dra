README = [
    "Všechny uvedené hodnoty jsou pouze orientační, nepřesnost odhaduji až k 20 %. U detekovaných incidentů mohou "
    "být některé události započítány vícekrát. Bohužel z dostupných dat nevím, jak lépe události filtrovat. "
    "Pracujeme na načítání logu RBC.",
    "Počty vlaků za den jsou určeny jako počty jedinečných čísel v každém dni, která se objevila ve FS.",
    "Počty vlaků za měsíc jsou součtem počtů vlaků v jednotlivých dnech v měsíci.",
    "Počty připojení jsou určeny jako počty jedinečných čísel vlaků v každém dni (nemusí se dostat do FS).",
    "Počty připojení za měsíc jsou součtem počtů připojení v jednotlivých dnech v měsíci.",
    "Počet hnacích vozidel je určen jako počet jedinečných OBU ID v daném dni / měsíci.",
    "Jako incident jsou započítány výskyty módů TR, PT a SF a rozpad spojení. Jsou odfiltrována opakující se "
    "hlášení stejného módu. Částečně je odfiltrováno přecházení mezi vyjmenovanými módy při jedné události. "
    "Sekvence přechodů FS > TR > PT > IS se započte jako 1 incident.",
    "Jako incident se nazpočítají přechody: TR > PT. Započítá se ale TR > x > PT (např. přes SB)",
    "Pro vyhodnocování dostupnosti systému doporučuji používat spíše ukazatel ovlivněných vlaků.",
    "Rozpad spojení je detekován jako zánik vlaku v módu FS či OS a následné opětovné připojení v SR pod stejným RBC.",
    "Zvlášť jsou uvedeny počty přechodů do módu IS."
    "Vlak je považován za vlak jedoucí po trati Olomouc - Uničov, pokud jeho číslo spadá do rozsahů 13701-13730, "
    "3621-3690, 12400-12560, 1438, 1439, 81730, 81371 nebo se připojil k RBC 101.",
    "V datech po měsících je měsíc určen prvním dnem. (Např. řádek s daty pro leden bude označen 2023-01-01 00:00:00)",
    "Počet vlaků ovlivněných incidentem je pro každý den stanoven jako počet různých čísel vlaků, "
    "u nichž se vyskytl incident (neuzohledňuje se výskyt módu IS).",
    "Počet vlaků ovlivněných incidentem za měsíc je stanoven jako součet počtů ovlivněných vlaků v jednotlivých "
    "dnech v měsíci",
    "Pro kumulativní hodnoty pro jednotlivá vozidla se průběžný součet provádí podle vozidla (tj. ve sloupci)."
    "Ve statistikách podle vozidel (HV) jsou sloupce seřazeny sestupně podle součtu počtů ovlivněných vlaků za celé "
    "sledované období (od 1.1.2023)"
]

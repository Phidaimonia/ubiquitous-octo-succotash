Pro měření rychlsoti jsem použil time.Monotonic.
Použité knihovny - pandas

Nejpomálejší částí bylo načítání CSV souborů, které jsem zrychlil hlavně použitím multiprocessoringu (víc řešení v notebooku).
Při psání kodu jsem se snažil co nejvíc používat metody pandas a vyhýbat se cyklům (kvůli vektorizaci výpočtů).
Skript jsem optimalizoval hlavně podle kritéria rychlosti, nebere ale v úvahu případné omezení na RAM, které by se asi dobře řešilo pomocí chunků.

Pro větší množství dat by se dalo uvažovat použití knihoven na distribuované výpočty, např. dask
Uložení všech invoiců do jednoho souboru by asi zrychlilo načítání na HDD
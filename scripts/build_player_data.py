#!/usr/bin/env python3
import csv
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

POS_KEYS=["P","C","1B","2B","3B","SS","LF","CF","RF","DH"]

def to_int(v):
    try:
        return int(v or 0)
    except Exception:
        return 0

def award_key(award_id):
    return {
        "All-Star Game":"allStar",
        "Most Valuable Player":"mvp",
        "Cy Young Award":"cyYoung",
        "Gold Glove":"goldGlove",
    }.get(award_id)

def load_rows(input_path, table, csv_name):
    if input_path.is_dir():
        with (input_path / csv_name).open(newline="", encoding="utf-8") as f:
            return list(csv.DictReader(f))
    conn=sqlite3.connect(str(input_path))
    conn.row_factory=sqlite3.Row
    rows=[dict(r) for r in conn.execute(f"SELECT * FROM {table}")]
    conn.close()
    return rows

def build(input_path, output_dir):
    people=load_rows(input_path,"People","People.csv")
    apps=load_rows(input_path,"Appearances","Appearances.csv")
    batting=load_rows(input_path,"Batting","Batting.csv")
    pitching=load_rows(input_path,"Pitching","Pitching.csv")
    awards=load_rows(input_path,"AwardsPlayers","AwardsPlayers.csv")

    players={}
    for p in people:
        pid=p.get("playerID")
        name=" ".join(filter(None,[p.get("nameFirst",""),p.get("nameLast","")])).strip()
        if not pid or not name:
            continue
        players[pid]={
            "id":pid,
            "displayName":name,
            "birthYear":to_int(p.get("birthYear")) or None,
            "debutYear":to_int((p.get("debut") or "")[:4]) or None,
            "finalYear":to_int((p.get("finalGame") or "")[:4]) or None,
            "appearances":{k:0 for k in POS_KEYS},
            "batting":defaultdict(lambda:{"hits":0,"homeRuns":0,"rbi":0,"runs":0,"stolenBases":0,"atBats":0,"gamesPlayed":0}),
            "pitching":defaultdict(lambda:{"wins":0,"strikeOuts":0,"saves":0,"inningsPitched":0.0,"gamesPlayed":0}),
            "awards":[],
            "awardTotals":{"allStar":0,"mvp":0,"cyYoung":0,"goldGlove":0},
        }

    for a in apps:
        p=players.get(a.get("playerID"))
        if not p: continue
        p["appearances"]["P"]+=to_int(a.get("G_p"))
        p["appearances"]["C"]+=to_int(a.get("G_c"))
        p["appearances"]["1B"]+=to_int(a.get("G_1b"))
        p["appearances"]["2B"]+=to_int(a.get("G_2b"))
        p["appearances"]["3B"]+=to_int(a.get("G_3b"))
        p["appearances"]["SS"]+=to_int(a.get("G_ss"))
        p["appearances"]["LF"]+=to_int(a.get("G_lf"))
        p["appearances"]["CF"]+=to_int(a.get("G_cf"))
        p["appearances"]["RF"]+=to_int(a.get("G_rf"))
        p["appearances"]["DH"]+=to_int(a.get("G_dh"))

    for r in batting:
        p=players.get(r.get("playerID")); year=to_int(r.get("yearID"))
        if not p or not year: continue
        y=p["batting"][year]
        y["hits"]+=to_int(r.get("H")); y["homeRuns"]+=to_int(r.get("HR")); y["rbi"]+=to_int(r.get("RBI"))
        y["runs"]+=to_int(r.get("R")); y["stolenBases"]+=to_int(r.get("SB")); y["atBats"]+=to_int(r.get("AB")); y["gamesPlayed"]+=to_int(r.get("G"))

    for r in pitching:
        p=players.get(r.get("playerID")); year=to_int(r.get("yearID"))
        if not p or not year: continue
        y=p["pitching"][year]
        y["wins"]+=to_int(r.get("W")); y["strikeOuts"]+=to_int(r.get("SO")); y["saves"]+=to_int(r.get("SV")); y["gamesPlayed"]+=to_int(r.get("G"))
        y["inningsPitched"]+=to_int(r.get("IPouts"))/3

    for r in awards:
        p=players.get(r.get("playerID"))
        if not p: continue
        key=award_key(r.get("awardID"))
        if key: p["awardTotals"][key]+=1
        p["awards"].append({"year":to_int(r.get("yearID")) or None,"awardID":r.get("awardID",""),"lgID":r.get("lgID","") or "","notes":r.get("notes","") or ""})

    output_dir.mkdir(parents=True, exist_ok=True)
    players_dir=output_dir / "players"
    players_dir.mkdir(parents=True, exist_ok=True)

    index=[]
    for p in players.values():
        primary=max(p["appearances"].items(), key=lambda kv: kv[1])[0]
        if p["appearances"][primary]==0: primary="DH"
        years=[y for y in [p["debutYear"],p["finalYear"]] if y]

        batting_y=[{"season":y,"stat":stat} for y,stat in sorted(p["batting"].items())]
        pitching_y=[{"season":y,"stat":stat} for y,stat in sorted(p["pitching"].items())]

        career_b=defaultdict(float)
        career_p=defaultdict(float)
        for row in batting_y:
            for k,v in row["stat"].items(): career_b[k]+=v
        for row in pitching_y:
            for k,v in row["stat"].items(): career_p[k]+=v

        payload={
            "id":p["id"],"displayName":p["displayName"],"birthYear":p["birthYear"],"debutYear":p["debutYear"],"finalYear":p["finalYear"],
            "primaryPos":primary,"isPitcher":p["appearances"]["P"]>0,
            "years":{"from":min(years) if years else None,"to":max(years) if years else None},
            "stats":{"batting":{"career":career_b,"yearByYear":batting_y},"pitching":{"career":career_p,"yearByYear":pitching_y}},
            "awards":{"totals":p["awardTotals"],"byYear":sorted(p["awards"], key=lambda a:a["year"] or 0)}
        }

        index.append({"id":p["id"],"displayName":p["displayName"],"years":payload["years"],"birthYear":p["birthYear"],"debutYear":p["debutYear"],"finalYear":p["finalYear"],"primaryPos":primary,"isPitcher":payload["isPitcher"]})
        (players_dir / f"{p['id']}.json").write_text(json.dumps(payload,separators=(",",":")))

    index.sort(key=lambda r: (-(r.get("finalYear") or 0), r["displayName"]))
    (output_dir / "players-index.json").write_text(json.dumps(index,separators=(",",":")))
    print(f"Wrote {len(index)} players into {output_dir}")

if __name__=="__main__":
    src=Path(sys.argv[1]) if len(sys.argv)>1 else Path("./lahman/core")
    out=Path(sys.argv[2]) if len(sys.argv)>2 else Path("./data")
    build(src, out)

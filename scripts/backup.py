#!/usr/bin/env python3
"""
Backup semanal do SmartPlantão.

Faz:
  1) pg_dump completo do banco (schema + dados) via DATABASE_URL
  2) Download de todos os buckets do Supabase Storage (inclui 'avatars', fotos, etc.)

Saída: diretório backup_<YYYY-MM-DD_HHMM>/ com database.sql e storage/<bucket>/...
"""
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    print("ERRO: pacote 'requests' não instalado. Rode: pip install requests", file=sys.stderr)
    sys.exit(1)


def env(key: str) -> str:
    v = os.environ.get(key)
    if not v:
        print(f"ERRO: variável de ambiente {key} ausente.", file=sys.stderr)
        sys.exit(1)
    return v


def main() -> None:
    DB_URL = env("DATABASE_URL")
    SB_URL = env("SUPABASE_URL").rstrip("/")
    SB_KEY = env("SUPABASE_SERVICE_KEY")

    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d_%H%M")
    out = Path(f"backup_{stamp}")
    out.mkdir(exist_ok=True)

    # ---------- 1. pg_dump ----------
    print(f"[1/2] pg_dump -> {out/'database.sql'}")
    subprocess.run(
        [
            "pg_dump",
            DB_URL,
            "--no-owner",
            "--no-acl",
            "--clean",
            "--if-exists",
            "-f",
            str(out / "database.sql"),
        ],
        check=True,
    )
    size_mb = (out / "database.sql").stat().st_size / 1024 / 1024
    print(f"      OK ({size_mb:.2f} MB)")

    # ---------- 2. Storage ----------
    print(f"[2/2] Supabase Storage -> {out/'storage'}")
    headers = {"Authorization": f"Bearer {SB_KEY}", "apikey": SB_KEY}

    r = requests.get(f"{SB_URL}/storage/v1/bucket", headers=headers, timeout=30)
    r.raise_for_status()
    buckets = r.json()

    if not buckets:
        print("      (nenhum bucket encontrado)")
    else:
        total_files = 0
        for bucket in buckets:
            bname = bucket["name"]
            bdir = out / "storage" / bname
            bdir.mkdir(parents=True, exist_ok=True)
            print(f"      bucket: {bname}")

            # Listagem recursiva (limite 1000 por chamada)
            offset = 0
            while True:
                lr = requests.post(
                    f"{SB_URL}/storage/v1/object/list/{bname}",
                    headers=headers,
                    json={"limit": 1000, "offset": offset, "prefix": "", "sortBy": {"column": "name", "order": "asc"}},
                    timeout=30,
                )
                lr.raise_for_status()
                items = lr.json() or []
                if not items:
                    break
                for obj in items:
                    name = obj.get("name")
                    if not name or obj.get("id") is None:
                        # pasta vazia ou metadata
                        continue
                    target = bdir / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    dr = requests.get(
                        f"{SB_URL}/storage/v1/object/{bname}/{name}",
                        headers=headers,
                        timeout=60,
                    )
                    if dr.status_code == 200:
                        target.write_bytes(dr.content)
                        total_files += 1
                    else:
                        print(f"        ! falha em {name}: HTTP {dr.status_code}", file=sys.stderr)
                if len(items) < 1000:
                    break
                offset += 1000
        print(f"      OK ({total_files} arquivos)")

    print(f"\n✅ Backup concluído: {out}")


if __name__ == "__main__":
    main()

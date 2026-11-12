"""Importa os flows de exemplo no NiFi na primeira subida do lab (serviço nifi-init).

Idempotente: se o process group já existe no canvas, não faz nada.
Para cada flow:
  1. faz upload de /flows/<nome>.json (exportado com "Download flow definition");
  2. preenche as propriedades sensíveis, que não vão no export (ex.: Secret Key do MinIO);
  3. habilita os controller services. Os processadores ficam PARADOS: dê Start na UI.

Só usa a biblioteca padrão do Python (roda dentro da imagem apache/nifi).
"""
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

BASE = os.environ.get("NIFI_URL", "https://nifi:8443") + "/nifi-api"
FLOWS = [f.strip() for f in os.environ.get("NIFI_IMPORT_FLOWS", "lab_example").split(",") if f.strip()]
FLOWS_DIR = os.environ.get("NIFI_FLOWS_DIR", "/flows")
# Propriedades sensíveis por nome de controller service
SENSITIVE = {
    "MinIO credentials": {
        "Access Key": os.environ.get("S3A_ACCESS_KEY", ""),
        "Secret Key": os.environ.get("S3A_SECRET_KEY", ""),
    },
}
CTX = ssl._create_unverified_context()  # certificado autoassinado do NiFi


def log(msg):
    print(f"[nifi-init] {msg}", flush=True)


def request(method, path, body=None, token=None, headers=None, raw=False):
    hdrs = dict(headers or {})
    if token:
        hdrs["Authorization"] = f"Bearer {token}"
    data = body
    if body is not None and not isinstance(body, (bytes, bytearray)):
        data = json.dumps(body).encode()
        hdrs.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(BASE + path, data=data, method=method, headers=hdrs)
    with urllib.request.urlopen(req, context=CTX, timeout=60) as r:
        out = r.read()
    if raw:
        return out.decode()
    return json.loads(out) if out else {}


def wait_for_token(timeout=600):
    form = urllib.parse.urlencode({
        "username": os.environ["NIFI_USERNAME"],
        "password": os.environ["NIFI_PASSWORD"],
    }).encode()
    deadline = time.time() + timeout
    while True:
        try:
            return request("POST", "/access/token", form, raw=True,
                           headers={"Content-Type": "application/x-www-form-urlencoded"})
        except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
            if time.time() > deadline:
                raise SystemExit(f"NiFi não respondeu em {timeout}s: {e}")
            log("aguardando o NiFi subir...")
            time.sleep(10)


def upload_flow(token, root, name, path):
    boundary = uuid.uuid4().hex
    fields = {"groupName": name, "positionX": "600", "positionY": "0",
              "clientId": str(uuid.uuid4()), "disconnectNode": "false"}
    parts = []
    for k, v in fields.items():
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode())
    with open(path, "rb") as f:
        content = f.read()
    parts.append(
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{name}.json"\r\n'
        f"Content-Type: application/json\r\n\r\n".encode() + content + b"\r\n"
    )
    parts.append(f"--{boundary}--\r\n".encode())
    return request("POST", f"/process-groups/{root}/process-groups/upload", b"".join(parts), token,
                   headers={"Content-Type": f"multipart/form-data; boundary={boundary}"})


def configure_services(token, group_id):
    services = request("GET", f"/flow/process-groups/{group_id}/controller-services"
                              "?includeAncestorGroups=false&includeDescendantGroups=true", token=token)
    for s in services["controllerServices"]:
        sid, name = s["id"], s["component"]["name"]
        props = SENSITIVE.get(name)
        if props and all(props.values()):
            cur = request("GET", f"/controller-services/{sid}", token=token)
            request("PUT", f"/controller-services/{sid}", {
                "revision": cur["revision"], "component": {"id": sid, "properties": props}}, token)
            log(f"  propriedades sensíveis preenchidas: {name}")
    # habilita depois de configurar (um serviço pode depender de outro)
    for _ in range(3):
        pending = 0
        services = request("GET", f"/flow/process-groups/{group_id}/controller-services"
                                  "?includeAncestorGroups=false&includeDescendantGroups=true", token=token)
        for s in services["controllerServices"]:
            c = s["component"]
            if c["state"] == "ENABLED":
                continue
            if c.get("validationStatus") != "VALID":
                pending += 1
                continue
            cur = request("GET", f"/controller-services/{s['id']}", token=token)
            request("PUT", f"/controller-services/{s['id']}/run-status",
                    {"revision": cur["revision"], "state": "ENABLED"}, token)
            log(f"  habilitado: {c['name']}")
        if not pending:
            return
        time.sleep(3)
    log("  atenção: há controller services inválidos; confira na UI")


def main():
    token = wait_for_token()
    root = request("GET", "/flow/process-groups/root", token=token)["processGroupFlow"]["id"]
    existing = {g["component"]["name"] for g in
                request("GET", f"/flow/process-groups/{root}", token=token)["processGroupFlow"]["flow"]["processGroups"]}
    for name in FLOWS:
        path = os.path.join(FLOWS_DIR, f"{name}.json")
        if name in existing:
            log(f"'{name}' já existe no canvas — nada a fazer")
            continue
        if not os.path.exists(path):
            log(f"arquivo {path} não encontrado — pulando")
            continue
        group = upload_flow(token, root, name, path)
        log(f"'{name}' importado ({group['id']})")
        configure_services(token, group["id"])
    log("pronto. Os processadores ficam parados: abra https://localhost:8443/nifi e dê Start no grupo.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

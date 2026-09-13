import json
import urllib.parse
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

# 1. Spark Session
spark = (
    SparkSession.builder.appName("Polaris_RBAC_Audit_Job")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

POLARIS_BASE_URL = (
    "http://polaris-catalog-service.bigdata.svc.cluster.local:8181"
)
CATALOG_NAME = "polaris"
ROOT_CLIENT_ID = "polaris_root"
ROOT_CLIENT_SECRET = "polaris_secret123"


def http_post(url, data_dict):
  encoded_data = urllib.parse.urlencode(data_dict).encode("utf-8")
  req = urllib.request.Request(
      url,
      data=encoded_data,
      headers={"Content-Type": "application/x-www-form-urlencoded"},
  )
  with urllib.request.urlopen(req, timeout=10) as resp:
    return json.loads(resp.read().decode("utf-8"))


def http_get(url, token):
  req = urllib.request.Request(
      url,
      headers={
          "Authorization": f"Bearer {token}",
          "Content-Type": "application/json",
      },
  )
  try:
    with urllib.request.urlopen(req, timeout=10) as resp:
      return resp.status, json.loads(resp.read().decode("utf-8"))
  except urllib.error.HTTPError as e:
    return e.code, {}


def get_admin_token():
  token_url = f"{POLARIS_BASE_URL}/api/catalog/v1/oauth/tokens"
  payload = {
      "grant_type": "client_credentials",
      "client_id": ROOT_CLIENT_ID,
      "client_secret": ROOT_CLIENT_SECRET,
      "scope": "PRINCIPAL_ROLE:ALL",
  }
  res = http_post(token_url, payload)
  return res["access_token"]


def fetch_full_rbac_matrix():
  token = get_admin_token()

  # A. Katalog Rolleri ve Grant'leri
  c_roles_url = (
      f"{POLARIS_BASE_URL}/api/management/v1/catalogs/{CATALOG_NAME}/catalog-roles"
  )
  status, c_data = http_get(c_roles_url, token)
  catalog_roles = [
      cr.get("name")
      for cr in (
          c_data.get("catalogRoles")
          or c_data.get("roles")
          or c_data.get("entities")
          or []
      )
  ]

  if not catalog_roles:
    catalog_roles = ["catalog_admin", "de_catalog_role", "ds_catalog_role", "etl_catalog_role"]

  grants_map = {}
  for c_role in catalog_roles:
    grants_url = f"{POLARIS_BASE_URL}/api/management/v1/catalogs/{CATALOG_NAME}/catalog-roles/{c_role}/grants"
    g_status, g_data = http_get(grants_url, token)
    if g_status == 200:
      privs = [
          f"{g.get('type')}:{g.get('privilege')}"
          for g in g_data.get("grants", [])
      ]
      grants_map[c_role] = ", ".join(privs) if privs else "NO_GRANTS"
    else:
      grants_map[c_role] = "NO_GRANTS"

  # B. Kullanıcılar (Principals) ve Rol Eşleşmeleri
  p_url = f"{POLARIS_BASE_URL}/api/management/v1/principals"
  p_status, p_data = http_get(p_url, token)
  principals = [p["name"] for p in p_data.get("principals", [])] if p_status == 200 else []

  records = []
  for user in principals:
    p_roles_url = f"{POLARIS_BASE_URL}/api/management/v1/principals/{user}/principal-roles"
    _, p_roles_data = http_get(p_roles_url, token)
    p_roles = [
        r.get("name")
        for r in (
            p_roles_data.get("principalRoles")
            or p_roles_data.get("roles")
            or p_roles_data.get("entities")
            or []
        )
    ]

    if not p_roles:
      records.append({
          "principal_name": user,
          "principal_role": "NONE",
          "catalog_name": CATALOG_NAME,
          "catalog_role": "NONE",
          "privileges": "NULL",
      })
      continue

    for p_role in p_roles:
      c_map_url = f"{POLARIS_BASE_URL}/api/management/v1/principal-roles/{p_role}/catalog-roles/{CATALOG_NAME}"
      _, c_map_data = http_get(c_map_url, token)
      c_roles = [
          cr.get("name")
          for cr in (
              c_map_data.get("catalogRoles")
              or c_map_data.get("roles")
              or c_map_data.get("entities")
              or []
          )
      ]

      if not c_roles:
        records.append({
            "principal_name": user,
            "principal_role": p_role,
            "catalog_name": CATALOG_NAME,
            "catalog_role": "NONE",
            "privileges": "NULL",
        })
      else:
        for cr in c_roles:
          records.append({
              "principal_name": user,
              "principal_role": p_role,
              "catalog_name": CATALOG_NAME,
              "catalog_role": cr,
              "privileges": grants_map.get(cr, "NO_GRANTS"),
          })

  return records


# 2. DataFrame Oluşturma ve Ekrana Basma
records = fetch_full_rbac_matrix()

schema = StructType([
    StructField("principal_name", StringType(), False),
    StructField("principal_role", StringType(), False),
    StructField("catalog_name", StringType(), False),
    StructField("catalog_role", StringType(), False),
    StructField("privileges", StringType(), True),
])

df_rbac = spark.createDataFrame(records, schema=schema)

df_rbac = df_rbac.orderBy("principal_name", "principal_role", "catalog_role")

print("\n" + "=" * 110)
print("                      POLARIS TÜM KULLANICI, ROL VE YETKİ MATRİSİ")
print("=" * 110)
df_rbac.show(truncate=False)
print("=" * 110)

spark.stop()
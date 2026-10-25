from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import duckdb
import pandas as pd
import requests

# 1. Récupération automatique des clés depuis le coffre-fort GitHub
MOODLE_URL = "https://e-learning-drd.moodlecloud.com"
TOKEN = os.environ["MOODLE_TOKEN"].strip()
MD_TOKEN = os.environ["MOTHERDUCK_TOKEN"].strip()

session = requests.Session()


def call_moodle_api(wsfunction, params=None):
  url = f"{MOODLE_URL}/webservice/rest/server.php"
  payload = {
      "wstoken": TOKEN,
      "wsfunction": wsfunction,
      "moodlewsrestformat": "json",
  }
  if params:
    payload.update(params)
  return session.post(url, data=payload, timeout=15).json()


# ==========================================
# ÉTAPE 1 : EXTRACTION PYTHON (API MOODLE)
# ==========================================
print("1/4 Extraction de l'API Moodle...")
raw_cat = call_moodle_api("core_course_get_categories")
raw_crs = call_moodle_api("core_course_get_courses")

if not isinstance(raw_crs, list) or len(raw_crs) == 0:
  raise ValueError(
      "Arrêt de sécurité : L'API Moodle ne répond pas. Les données MotherDuck"
      " restent intactes."
  )

df_categories = pd.DataFrame(raw_cat)[
    ["id", "name", "parent", "coursecount", "depth", "path"]
]
df_courses = pd.DataFrame(raw_crs)
df_courses = df_courses[df_courses["id"] != 1][
    [
        "id",
        "shortname",
        "fullname",
        "categoryid",
        "format",
        "startdate",
        "timecreated",
        "timemodified",
        "visible",
    ]
]

modules_list, teachers_list = [], []


def fetch_details(course_id):
  c_mods, c_teachers = [], []
  contents = call_moodle_api("core_course_get_contents", {"courseid": course_id})
  if isinstance(contents, list):
    for section in contents:
      mods = section.get("modules", [])
      if not mods:
        c_mods.append({
            "course_id": course_id,
            "section_number": section.get("section"),
            "section_name": section.get("name"),
            "module_id": None,
            "module_name": None,
            "module_type": "empty_section",
        })
      else:
        for mod in mods:
          c_mods.append({
              "course_id": course_id,
              "section_number": section.get("section"),
              "section_name": section.get("name"),
              "module_id": mod.get("id"),
              "module_name": mod.get("name"),
              "module_type": mod.get("modname"),
          })
  enrolled = call_moodle_api(
      "core_enrol_get_enrolled_users", {"courseid": course_id}
  )
  if isinstance(enrolled, list):
    for user in enrolled:
      roles = [r.get("shortname") for r in user.get("roles", [])]
      if "editingteacher" in roles or "teacher" in roles:
        c_teachers.append({
            "course_id": course_id,
            "teacher_id": user.get("id"),
            "username": user.get("username"),
            "fullname": user.get("fullname"),
            "email": user.get("email"),
            "role": roles[0],
        })
  return c_mods, c_teachers


with ThreadPoolExecutor(max_workers=10) as executor:
  futures = [executor.submit(fetch_details, cid) for cid in df_courses["id"]]
  for future in as_completed(futures):
    m, t = future.result()
    modules_list.extend(m)
    teachers_list.extend(t)

df_course_contents = pd.DataFrame(modules_list)
df_course_teachers = pd.DataFrame(teachers_list)

# ==========================================
# ÉTAPE 2 : CHARGEMENT BRONZE (MOTHERDUCK)
# ==========================================
print("2/4 Connexion à MotherDuck et chargement Bronze...")
con = duckdb.connect(f"md:?motherduck_token={MD_TOKEN}")
con.execute("CREATE DATABASE IF NOT EXISTS moodle_analytics;")
con.execute("USE moodle_analytics;")

con.execute("CREATE SCHEMA IF NOT EXISTS moodle_bronze;")
con.execute("CREATE SCHEMA IF NOT EXISTS moodle_silver;")
con.execute("CREATE SCHEMA IF NOT EXISTS moodle_gold;")

con.execute(
    "CREATE OR REPLACE TABLE moodle_bronze.raw_categories AS SELECT * FROM"
    " df_categories"
)
con.execute(
    "CREATE OR REPLACE TABLE moodle_bronze.raw_courses AS SELECT * FROM"
    " df_courses"
)
con.execute(
    "CREATE OR REPLACE TABLE moodle_bronze.raw_course_contents AS SELECT * FROM"
    " df_course_contents"
)
con.execute(
    "CREATE OR REPLACE TABLE moodle_bronze.raw_course_teachers AS SELECT * FROM"
    " df_course_teachers"
)

# ==========================================
# ÉTAPE 3 : TRANSFORMATION SQL SILVER
# ==========================================
print("3/4 Transformation SQL (Couche Silver)...")
con.execute("""
CREATE OR REPLACE TABLE moodle_silver.stg_courses AS
SELECT 
    c.id AS course_id,
    c.shortname,
    c.fullname AS course_name,
    c.categoryid,
    cat.name AS category_name,
    cat.path AS category_path,
    c.visible,
    to_timestamp(c.timecreated) AS created_at,
    to_timestamp(c.timemodified) AS updated_at
FROM moodle_bronze.raw_courses c
LEFT JOIN moodle_bronze.raw_categories cat ON c.categoryid = cat.id;
""")

# ==========================================
# ÉTAPE 4 : MODÉLISATION SQL GOLD (POUR POWER BI)
# ==========================================
print("4/4 Création de la table finale Gold pour Power BI...")
con.execute("""
CREATE OR REPLACE TABLE moodle_gold.audit_cours AS
SELECT 
    c.course_id,
    c.shortname,
    c.course_name,
    c.category_name,
    c.visible,
    c.updated_at,
     COALESCE( STRING_AGG(DISTINCT t.fullname, ', '), 'Aucun enseignant') AS enseignants,
    COUNT(DISTINCT CASE WHEN m.module_type = 'resource' THEN m.module_id END) AS nb_fichiers_pdf,
    COUNT(DISTINCT CASE WHEN m.module_type = 'assign' THEN m.module_id END) AS nb_devoirs,
    COUNT(DISTINCT CASE WHEN m.module_type = 'quiz' THEN m.module_id END) AS nb_quiz,
    COUNT(DISTINCT CASE WHEN m.module_type = 'forum' THEN m.module_id END) AS nb_forums,
    COUNT(DISTINCT m.module_id) AS total_modules,
    CASE 
        WHEN COUNT(DISTINCT m.module_id) = 0 THEN '1. Vide'
        WHEN COUNT(DISTINCT CASE WHEN m.module_type IN ('assign', 'quiz') THEN m.module_id END) > 0 THEN '3. Complet (Cours + Évaluation)'
        ELSE '2. Basique (Support uniquement)'
    END AS statut_pedagogique
FROM moodle_silver.stg_courses c
LEFT JOIN moodle_bronze.raw_course_contents m ON c.course_id = m.course_id
LEFT JOIN moodle_bronze.raw_course_teachers t ON c.course_id = t.course_id
GROUP BY 1, 2, 3, 4, 5, 6;
""")

print("✅ PIPELINE TERMINÉ AVEC SUCCÈS SUR MOTHERDUCK !")

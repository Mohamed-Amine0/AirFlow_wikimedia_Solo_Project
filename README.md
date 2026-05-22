# Plateforme d'Analyse Wikimedia

Projet de pipeline Big Data complet pour l'ingestion, le traitement et l'analyse du flux d'événements Wikimedia en temps réel.

## Objectif

Reproduire une version simplifiée de la plateforme d'analyse de Wikimedia basée sur le flux public:
https://stream.wikimedia.org/v2/stream/recentchange

Ce flux contient les événements réels de Wikipédia: éditions, suppressions, créations de pages, activités de bots, etc.

## Demo

Capture du tableau de bord réel incluse dans le dépôt:

![Dashboard demo](docs/demo/screenshot-1.png)

## Architecture

```
Wikimedia EventStreams
      |
      v
Ingestion Service (Python)
      |
      v
Kafka (raw events)
      |
      v
Airflow (orchestration)
      |
      v
Traitement et Agrégations
      |
      v
Stockage local/HDFS
      |
      v
Rapports + Dashboards
```

## Composants

### DAGs Airflow

#### 1. wikimedia_ingestion_kafka

Consomme le flux Wikimedia en temps réel et l'envoie vers Kafka.

Fonctionnalités:
- Récupération continue des événements Wikimedia
- Parsing et validation JSON
- Classification des événements:
  - Type utilisateur: bot vs humain
  - Type compte: anonyme vs connecté
  - Type action: création, édition, suppression
- Enrichissement avec métadonnées (timestamp, source, partition_key)
- Gestion des erreurs et routing vers topic wm.errors
- Logging détaillé des événements rejetés

Topics Kafka générés:
- `wm.recentchange.raw`: Tous les événements bruts
- `wm.bot.events`: Événements bots uniquement
- `wm.page.edits`: Éditions et créations de pages
- `wm.errors`: Événements invalides

Fréquence: Toutes les 5 minutes

#### 2. wikimedia_traitement_agregations

Traite les événements et génère des agrégations statistiques.

Agrégations:
- Activité générale:
  - Nombre d'événements par minute/heure
  - Distribution par langue
  - Distribution par wiki
- Pages principales:
  - Pages les plus modifiées (top 20)
  - Pages les plus créées (top 20)
  - Pages les plus supprimées (top 10)
- Analyse utilisateurs:
  - Top 20 contributeurs
  - Ratio anonymes/connectés
  - Ratio bots/humains
- Activité bots:
  - Volume total édits bots
  - Bots les plus actifs (top 20)
  - Nombre de bots uniques

Fichiers générés:
- `activity_by_hour.json`
- `top_pages.json`
- `user_activity.json`
- `bot_ratio.json`
- `language_distribution.json`

Fréquence: Toutes les heures

#### 3. wikimedia_detection_anomalies

Détecte les anomalies et problèmes de qualité données.

Types d'anomalies détectées:

1. Spikes d'activité
   - Augmentation > 300% en < 5 minutes
   - Sévérité: high/medium selon pourcentage

2. Comportements anormaux de bots
   - Bots effectuant > 100 édits/minute
   - Bots éditant pages protégées (MediaWiki:, Template:)
   - Sévérité: high

3. Spam et vandalisme
   - Pages modifiées 5+ fois en < 2 minutes
   - Reverts massifs (3+ reverts)
   - Sévérité: high/medium

4. Anomalies données
   - Événements sans page_id
   - Timestamps incohérents
   - Langue inconnue
   - Wiki null
   - Sévérité: low

Fichier généré:
- `anomalies.jsonl`

Fréquence: Toutes les heures

#### 4. wikimedia_reporting

Génère les rapports automatisés consolidant toutes les données.

Rapports générés:

1. Rapport d'activité globale
   - Nombre total d'événements
   - Top pages et contributeurs
   - Ratio bots/humains

2. Rapport qualité données
   - Anomalies détectées
   - Anomalies par type et sévérité
   - Taux données invalides

3. Rapport trafic
   - Trafic par heure
   - Distribution par langue
   - Distribution par wiki

4. Rapport système
   - Status des pipelines
   - Latences observées
   - Taux d'erreurs

Fichier généré:
- `rapport_YYYY-MM-DD.json`

Fréquence: Quotidien à 2h du matin

## Structure du Projet

```
airflow-project/
├── dags/
│   ├── wikimedia_ingestion_kafka.py
│   ├── wikimedia_traitement_agregations.py
│   ├── wikimedia_detection_anomalies.py
│   ├── wikimedia_reporting.py
│   └── weather_*.py (exercices existants)
├── config/
│   └── airflow.cfg
├── data/
│   ├── weather_intervals/
│   ├── weather_reports/
│   └── weather_snapshots/
├── logs/
├── plugins/
├── docker-compose.yaml
├── requirements.txt
└── README.md
```

## Installation et Démarrage

### Prérequis

- Docker et Docker Compose
- Python 3.10+
- Git

### Installation locale

1. Cloner le repository:
```bash
git clone https://github.com/Mohamed-Amine0/AirFlow_wikimedia_Solo_Project.git
cd AirFlow_wikimedia_Solo_Project
```

2. Installer les dépendances Python:
```bash
pip install -r requirements.txt
```

3. Initialiser la base de données Airflow:
```bash
airflow db init
```

4. Créer un utilisateur admin:
```bash
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com
```

5. Démarrer Airflow:
```bash
airflow webserver --port 8080
airflow scheduler
```

### Avec Docker Compose

1. Démarrer les services:
```bash
docker-compose up -d
```

2. Accéder à l'interface Airflow:
```
http://localhost:8080
```

Identifiants par défaut:
- Username: airflow
- Password: airflow

## Utilisation

### Interface Web Airflow

1. Accéder à http://localhost:8080
2. Les DAGs Wikimedia apparaissent dans la liste
3. Cliquer sur un DAG pour voir les détails
4. Déclencher manuellement un DAG:
   - Bouton "Trigger DAG" en haut à droite
   - Ou programmer l'exécution automatique

### Visualisation des résultats

Les fichiers générés sont stockés dans:
- `/tmp/airflow_wikimedia_data/` (développement local)

Pour consulter les résultats:
```bash
# Voir les événements bruts
cat /tmp/airflow_wikimedia_data/wm.recentchange.raw_*.jsonl

# Voir les agrégations
cat /tmp/airflow_wikimedia_data/top_pages.json
cat /tmp/airflow_wikimedia_data/user_activity.json

# Voir les anomalies
cat /tmp/airflow_wikimedia_data/anomalies.jsonl

# Voir les rapports
cat /tmp/airflow_wikimedia_data/rapport_*.json
```

### Dashboard Temps Réel (OBLIGATOIRE)

Un dashboard complet pour visualiser le pipeline en temps réel.

#### Démarrage du dashboard

1. Générer les données de test (facultatif):
```bash
python generate_test_data.py
```

2. Lancer le serveur dashboard:
```bash
python serve_dashboard.py
```

3. Accéder au dashboard:
```
http://localhost:8000
```

#### Sections du dashboard

Le dashboard affiche en temps réel:

**Streaming**
- Événements par seconde
- Top 5 pages modifiées en direct
- Activité bots (nombre, édits, ratio)

**Kafka**
- Topics disponibles (wm.recentchange.raw, wm.bot.events, wm.page.edits, wm.errors)
- Lag de consommation
- Nombre de brokers et status

**Airflow - Orchestration**
- État des 4 DAGs Wikimedia
- Nombre de tâches réussies/échouées
- DAGs actifs et latences

**Qualité des Données**
- Anomalies détectées
- Taux de données invalides
- Distribution par langue (top 10)
- Activité par heure

**Alertes et Incidents**
- Liste des alertes actives
- Sévérité des incidents
- Messages d'alerte détaillés

#### Caractéristiques

- Auto-rafraîchissement chaque 5 secondes
- Interface responsive (desktop/mobile)
- Chargement en temps réel des données JSON
- Thème sombre professionnel
- Indicateurs de status (sain/avertissement/erreur)

#### Architecture du dashboard

```
Dashboard HTML (dashboard.html)
      |
      v
Serveur HTTP (serve_dashboard.py)
      |
      v
Fichiers JSON
├── activity_by_hour.json (activité)
├── top_pages.json (pages)
├── user_activity.json (utilisateurs)
├── bot_ratio.json (bots)
├── language_distribution.json (langues)
├── anomalies.jsonl (anomalies)
├── monitoring_latest.json (métriques Airflow/Kafka)
└── rapport_YYYY-MM-DD.json (rapport consolidé)
```

Le dashboard consomme automatiquement tous ces fichiers et les affiche de manière claire et accessible.

## Flux de traitement

1. **Ingestion (wikimedia_ingestion_kafka)**
   - Consomme le flux Wikimedia
   - Valide et classifie les événements
   - Les événements invalides vont dans wm.errors

2. **Traitement (wikimedia_traitement_agregations)**
   - Charge les données brutes
   - Génère agrégations (activité, pages, utilisateurs)
   - Crée fichiers JSON pour chaque catégorie

3. **Détection (wikimedia_detection_anomalies)**
   - Analyse les spikes d'activité
   - Détecte comportements bots anormaux
   - Identifie spam et vandalisme
   - Signale anomalies données

4. **Reporting (wikimedia_reporting)**
   - Combine toutes les données
   - Génère rapports consolidés
   - Crée rapport quotidien

## Dépendances entre DAGs

```
wikimedia_ingestion_kafka
      |
      v
wikimedia_traitement_agregations
      |
      ├---> wikimedia_detection_anomalies
      |     |
      |     v
      +--> wikimedia_reporting
```

Les DAGs de traitement, détection et reporting peuvent être déclenchés manuellement après l'ingestion.

## Configuration

### Variables d'environnement

- `AIRFLOW_UID`: User ID dans les conteneurs (défaut: 50000)
- `AIRFLOW_PROJ_DIR`: Répertoire du projet (défaut: .)
- `_AIRFLOW_WWW_USER_USERNAME`: Username admin (défaut: airflow)
- `_AIRFLOW_WWW_USER_PASSWORD`: Password admin (défaut: airflow)

### Fichier .env


## Demo

Captures d'écran du tableau de bord (dossier `docs/demo`).

- **But**: fournir une preuve visuelle du dashboard temps réel pour la livraison.
- **Contenu attendu**: ajoutez une ou plusieurs images nommées `screenshot-1.png`, `screenshot-2.png`, etc. dans `docs/demo`.

Exemple d'inclusion dans ce README:

![Dashboard demo](docs/demo/screenshot-1.png)

Conserver la section `Demo` concise — pas d'emojis, pas de décorations inutiles.

Copier `.env.example` en `.env` et adapter les variables si besoin.

## Développement

### Format des événements Wikimedia

Les événements reçus ont la structure suivante:
```json
{
  "type": "edit|new|delete",
  "wiki": "enwiki|frwiki|...",
  "page_id": 12345,
  "page_title": "Article Title",
  "timestamp": "2026-05-20T12:34:56Z",
  "user": "Username",
  "bot": false,
  "comment": "Edit summary",
  "meta": {
    "domain": "en.wikipedia.org"
  }
}
```

### Extension des DAGs

Pour ajouter une nouvelle tâche à un DAG:

1. Décorer la fonction avec `@task`
2. Définir les entrées/sorties
3. Ajouter à l'orchestration du DAG

Exemple:
```python
@task
def ma_tache(donnees: List[Dict]) -> Dict:
    """Traiter les données"""
    resultat = {}
    for item in donnees:
        # Traitement
        pass
    return resultat

# Orchestration
resultat = ma_tache(donnees_sources)
```

## Limitations et améliorations futures

### Limitations actuelles

- Simulation du stockage Kafka (fichiers locaux)
- Pas d'intégration HDFS réelle
- Pas de dashboard web
- Données simulées en développement

### Améliorations futures

- Intégration Kafka réelle avec confluent-kafka
- Stockage HDFS pour données volumineuses
- Dashboard Grafana pour visualisation temps réel
- Alertes en cas d'anomalies critiques
- Métriques Prometheus
- Tests unitaires et intégration

## Contributions

Les contributions sont bienvenues. Pour contribuer:

1. Fork le repository
2. Créer une branche feature (`git checkout -b feature/ma-feature`)
3. Commit les changements (`git commit -am 'Ajouter ma-feature'`)
4. Push vers la branche (`git push origin feature/ma-feature`)
5. Ouvrir une Pull Request

## Auteur

Mohamed Amine0

## Licence

Ce projet est fourni à titre d'exemple pédagogique.

## Ressources

- Flux Wikimedia: https://stream.wikimedia.org/
- Documentation Airflow: https://airflow.apache.org/docs/
- Documentation Kafka: https://kafka.apache.org/documentation/
- Wikimedia API: https://www.mediawiki.org/wiki/API:Main_page

## Support

Pour les problèmes:
1. Vérifier les logs Airflow (`docker logs airflow-scheduler`)
2. Vérifier les fichiers de données générés
3. Ouvrir une issue sur GitHub

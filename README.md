rethinkdb-analytics
===================

Analytics tools of all sorts.

## Setup

1. Create and activate virtual environment

```
virtualenv venv
source venv/bin/activate
```

2. Install Python depdendencies

```
pip install -f requirements.pip
```

3. Install Bower dependencies

```
npm install -g bower 
bower install 
```

## Generating Analytics

1. Generating number of users

```
python generate_results.py month
```

2. Generate number of GitHub stars

```
python generate_github_stars.py --username GH_USERNAME --password GH_PASSWORD
```

## View Analaytics

Server dashboard by using the Flask app.

```
python app.py
```

and then go to [http://localhost:5000](http://localhost:5000).

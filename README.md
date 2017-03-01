# Data

Keeps track of sources and issues related to data in FireCARES.

* NFIRS: [National Fire Incident Reporting System](sources/nfirs/README.md)
* USFA: [US Fire Administration](sources/usfa/README.md)

see [terms](terms.md) for additional information regarding terminology used in FireCARES

## IPython/Jupyter notebook setup

### Database connections

`firecares`/`nfirs`/`parcels` should be connections defined in your [.pg_service.conf](https://www.postgresql.org/docs/9.0/static/libpq-pgservice.html), pointed at the respective databases.

### Python

_We'd recommend creating a [virtualenv](https://virtualenv.pypa.io/en/stable/) to contain your python libraries specific to this project._

Install the necessary python dependencies via:

```bash
pip install -r requirements.txt
```

Next, run `ipython notebook` to start a notebook session, from here you'll be able to navigate to the notebook you'd like to open via your browser.

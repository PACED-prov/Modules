# ACED&mdash;Modules

This repository contains all the necessary modules to filter, transform, query, and build entity flow graphs (EFGs) of the entities associated with a cross-namespace event. These cross-namespace events can be detected through SPADE's [CrossNamespaces](https://github.com/ashish-gehani/SPADE/wiki/Available-filters#crossnamespaces) filter. Morever, the repository also contains modules to extract features from the EFGs. We have also added custom configuration files which can serve as as sample. 

<br>

| Category           | Name                        | Purpose                                                                                    |
|--------------------|-----------------------------|--------------------------------------------------------------------------------------------|
| Preprocessing      | sortlog_camflow.py          | Sorts camflow based on relation ids so that SPADE's CrossNamespaces filter can ingested it |
| Querying           | EFGquerygenertor_spade.py   | Generates a SPADE query script that builds EFGs                                            |
| Feature Extraction | extractor_privilegedflow.py | Extracts privileged_flow feature for anomaly detection                                     |
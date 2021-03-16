# Rudimentary Filecoin+ dealflow analysis

This repository contains a simple perl program that groups and tabulates existing Filecoin+ deals across known clients and miners. The labeling of client wallets and miner actors is sourced from the current [public notary registry](https://github.com/filecoin-project/filecoin-plus-client-onboarding) and also [its older version]( https://github.com/keyko-io/filecoin-clients-onboarding).

The only "filtering" this program performs is when assembling `filp_miner_stats.csv` it does not consider any miners with less than 1TiB of Fil+ data currently proving.

## To regenerate the reports using current data, execute:

`./regenerate_stats.pl`

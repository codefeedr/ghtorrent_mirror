# GHTorrent Mirror plugin
The goal of this plugin is to mirror GHTorrent. 

## Architecture

## Experimental

### Commit enrichment
The idea of this stage is to enrich Commit data with their corresponding PushEvent. I.e. Add the `push_id` and `push_date` to a Commit.
The flow diagram of this low-level join can be found below:

![](flow_enrich.png)
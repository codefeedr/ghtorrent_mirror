# GHTorrent Mirror plugin
The goal of this plugin is to mirror GHTorrent. 

## Architecture

## Experimental

### Commit enrichment
The idea of this stage is to enrich Commit data with their corresponding PushEvent (if available). I.e. Add the `push_id` and `push_date` to a Commit.

The commit is enriched in the following case class:
```scala
  case class Pushed(push_id: Long,
                    push_date: Date,
                    pushed_from_github: Boolean = false)

  case class EnrichedCommit(push: Option[Pushed], commit: Commit)
```
All data is pushed into the `cf_commit` topic.


The flow diagram of this low-level join can be found below:
![](flow_enrich.png)

The potential outcomes are:
- PushEvent can be found and Commit is enriched with `push_id` and `push_date`.
- PushEvent can not be found but it is derived that Commit is directly pushed from GH. The Commit is then enriched with no `push_id` and the `push_date == commit_date`.
- No PushEvent can be found and Commit is just forwarded without `Pushed` data.
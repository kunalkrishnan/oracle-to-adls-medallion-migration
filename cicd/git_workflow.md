Git Branching Strategy

main
develop
feature/*
release/*
hotfix/*

Workflow

Developer creates feature branch

feature/oracle-bronze-ingestion

Code is committed and pushed.

Pull Request created to develop branch.

After testing → merged to develop.

Release branch created

release/v1.0

After approval → merged to main.

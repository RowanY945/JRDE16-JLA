# JRDE16-JLA

## 🔀 Branching Strategy
### Branch Naming Convention
All branches must follow this exact pattern:\
`[prefix]/[ticket-number]-[task-name]`
### Prefixes

`feature` - New features and enhancements\
`bugfix` - Bug fixes\
`hotfix` - Critical production fixes\
`refactor` - Code improvements without functionality changes\
`docs` - Documentation updates

### Examples

`feature/PROJ-123-user-authentication`\
`bugfix/PROJ-456-login-validation-error`

### Branch Naming Rules

Use lowercase for everything\
Use hyphens to separate words in task names\
Keep task names descriptive but concise\
Always include the full ticket number\
No spaces or special characters except hyphens

## 📝 Commit Guidelines
**One Commit Per Pull Request**
Important: Each pull request must contain exactly **ONE** commit. Use git rebase -i or git reset to squash multiple commits before creating the PR.\
Commit Message Pattern\
`[prefix]: [task name] [task description]`

`Resolve [ticket-number]`
### Commit Message Structure

`Line 1: [prefix]: [task name] [task description]`\
`Line 2: Empty line`\
`Line 3: Resolve [ticket-number]`

### Examples
`feature: User Authentication Implement JWT-based login system with password validation`

`Resolve PROJ-123`\
`bugfix: Login Validation Fix email format validation regex pattern`

`Resolve PROJ-456`\
`hotfix: Security Patch Update dependency versions to fix vulnerability`

## 🏗️ Requirements
Prefix must match the branch prefix\
Task name should be title case and descriptive\
Task description should explain what was done\
Always include "Resolve [ticket-number]" on the last line\
No period at the end of the first line\
Keep first line under 72 characters when possible\

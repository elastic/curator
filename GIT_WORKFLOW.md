# Git Workflow for RC Development

## Context
After submitting PR from RC1 branch to upstream, continuing development that builds upon RC1 changes.

## Recommended Approach: Branch from RC1

### Create new branch from RC1
```bash
git checkout -b RC2 RC1
```

### Benefits
- Continue developing immediately with the RC1 foundation
- Keep RC1 frozen for the PR review process
- Maintain flexibility for PR changes

### Workflow

1. **Develop new features on RC2**
   ```bash
   git checkout RC2
   # Make changes, commit as normal
   ```

2. **If RC1 needs changes from PR review:**
   ```bash
   # Switch back to RC1
   git checkout RC1

   # Make requested changes
   # Commit changes

   # Push updates to PR
   git push origin RC1
   ```

3. **Sync RC2 with updated RC1:**
   ```bash
   git checkout RC2
   git rebase RC1
   ```

4. **After RC1 is merged upstream:**
   ```bash
   # Sync master with upstream
   git checkout master
   git fetch upstream
   git merge upstream/master
   git push origin master

   # Rebase RC2 onto master
   git checkout RC2
   git rebase master

   # Clean up merged RC1 branch
   git branch -d RC1
   git push origin --delete RC1
   ```

## Alternative Naming
Instead of RC2, consider more descriptive names:
- `deepfreeze-phase2`
- `feature/deepfreeze-enhancements`
- `RC2-deepfreeze-completion`

## Other Approaches Considered

### New branch from master (for independent work)
```bash
git checkout master
git checkout -b feature/new-feature
```
**Use when:** Next work is independent of RC1 changes

### Wait for PR merge (most conservative)
Wait until PR is accepted, sync with upstream, then branch
**Use when:** No urgency and want clean linear history

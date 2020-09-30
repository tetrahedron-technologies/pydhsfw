# Welcome
Welcome and thank you for contributing!

## Git Procedures
Generally we use the Git Feature Branch Workflow described here:  
https://www.atlassian.com/git/tutorials/comparing-workflows/feature-branch-workflow

To summarize:
1. Each change should be tied to a github issue tracker that describes the enhancement, feature, bug, etc. If one doesn't exist, please create one. 
There may be exceptions but let's try to avoid them.
2. When working on a feature make sure to pull the latest code so you have a fresh version to work from.  
`git pull`
3. Create a local branch to work on your code. Use this naming convention `gh<issue#>_<first_few_words_of_issue_title>`.
Ultimately these branches will be removed but it's nice to have them sort in a group when viewing branches.  
`git checkout -b gh10_use_decorators_to_register_messages`
4. Make your code changes. Use `git add <file>` and `git commit -m` liberally during development.
5. Before pushing to the origin it's nice to pull any changes from the origin to make sure there are no merge conflics.  
`git pull origin master`
6. Now you can push to the origin.  
`git push origin gh10_use_decorators_to_register_messages`
7. Then use the Github UI to create a pull request.  https://docs.github.com/en/github/collaborating-with-issues-and-pull-requests/creating-a-pull-request. Name the title of the pull request `GH-<issue#> <title of the issue>`. Add a brief description about changes to the code or notes that reviewer may need It's less about repeating what's in the original issue, but for simple changes they may be similar. 
For example, `GH-10 Use decorators to register messages with message factories`. This convention is supposed to autolink the pull request with the issue.
8. Discuss, review, assign reviewers, comment, edit code, etc. in Github until code it's ready to be merged. 
Please expect to receive constructive comments and code change requests, this is very normal, don't worry you are amazing and so is your code.
9. After reviewers have approved the request, the contributor that created the pull request should merge it.


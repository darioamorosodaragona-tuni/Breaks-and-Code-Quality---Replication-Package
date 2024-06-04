from github import Github, PaginatedList, TimelineEvent
from github.Issue import Issue
from github.PullRequest import PullRequest


def get_linked_pr_from_issue(issue: Issue) -> PullRequest | None:
    """Check if the given issue has a linked pull request.

  This function iterates over the timeline of the issue and checks if there is a 'cross-referenced' event.
  If such an event is found, it checks if the source of the event is an issue and if so, it returns the issue as a pull request.

  Usage:
  issue: Issue = repo.get_issue(number=8)
  pr_or_none = check_if_issue_has_linked_pr(issue)

  Args:
      issue (Issue): The issue to check for a linked pull request.

  Returns:
      PullRequest: The linked pull request if it exists, None otherwise.
  """
    events_pages: PaginatedList[TimelineEvent] = issue.get_timeline()
    pg_num = 0
    while events_pages.get_page(pg_num):
        page = events_pages.get_page(pg_num)
        pg_num += 1
        for e in page:
            if str(e.event) == 'cross-referenced':
                if e.source and e.source.issue:
                    return e.source.issue.as_pull_request()


def get_linked_issue_from_pr(pr: PullRequest) -> Issue | None:
    """Check if the given pull request has a linked issue.

  This function iterates over the timeline of the pull request and checks if there is a 'cross-referenced' event.
  If such an event is found, it checks if the source of the event is a pull request and if so, it returns the pull request as an issue.

  Usage:
  pr: PullRequest = repo.get_pull(number=8)
  issue_or_none = check_if_pr_has_linked_issue(pr)

  Args:
      pr (PullRequest): The pull request to check for a linked issue.

  Returns:
      Issue: The linked issue if it exists, None otherwise.
  """
    events_pages: PaginatedList[TimelineEvent] = pr.as_issue().get_timeline()
    pg_num = 0
    while events_pages.get_page(pg_num):
        page = events_pages.get_page(pg_num)
        pg_num += 1
        for e in page:
            if str(e.event) == 'cross-referenced':
                if e.source and e.source.issue:
                    return e.source.issue


from github import Auth

# using an access token
# TODO: change the GITHUB token

auth = Auth.Token("[GIT_TOKEN]")

# Public Web Github
g = Github(auth=auth)

repo = g.get_repo("apache/jmeter")

issue: Issue = repo.get_issue(number=6219)
pr_or_none = get_linked_issue_from_pr(repo.get_pull(number=6220))
print(pr_or_none)

# How to contribute

## Please note:

* (Optional) We adopt [Git Flow](https://www.atlassian.com/git/tutorials/comparing-workflows/gitflow-workflow). Most feature branches are pushed to the repository and deleted when merged to *develop* branch.
* (**Important**): Submit pull requests to the *develop* branch or *feature/* branches
* Use *GitHub Issues* for feature requests and bug reports. Include as much information as possible while reporting bugs.  


## Contributing (Step-by-step)

1. [Fork the repo](http://help.github.com/fork-a-repo) and clone it to your local computer, and set up the upstream remote:

        git clone https://github.com/YourGithubUsername/pyomop.git
        cd pyomop
        git remote add upstream https://github.com/dermatologist/pyomop.git

2. Checkout out a new local branch based on your master and update it to the latest (TRUNK-123 is the branch name, You can name it whatever you want. Try to give it a meaningful name. If you are fixing an issue, please include the issue #).

        git checkout -b BRANCH-123 develop
        git clean -df
        git pull --rebase upstream develop

 > Please keep your code clean. If you find another bug, you want to fix while being in a new branch, please fix it in a separated branch instead.

3. Push the branch to your fork. Treat it as a backup.

        git push origin BRANCH-123

4. Code

  * Adhere to common conventions you see in the existing code.
  * Include tests as much as possible, and ensure they pass.

5. Commit to your branch

         git commit -m "BRANCH-123: Put change summary here (can be a ticket title)"

  **NEVER leave the commit message blank!** Provide a detailed, clear, and complete description of your commit!

6. Update your branch to the latest code.
  
        git pull --rebase upstream develop

7. **Important** If you have made many commits, please squash them into atomic units of work. (Most Git GUIs such as sourcetree and smartgit offer a squash option)

       
        git checkout develop
        git pull --rebase upstream develop
        git merge --squash BRANCH-123
        git commit -m "fix: 123"

  Push changes to your fork:

        git push

8. Issue a Pull Request

  In order to make a pull request:
  * Click "Pull Request".
  * Choose the develop branch
  * Click 'Create pull request'
  * Fill in some details about your potential patch including a meaningful title.
  * Click "Create pull request".

  Thanks for that -- we'll get to your pull request ASAP. We love pull requests!

## Feedback

   If you need to contact me, see my contact details on my profile page.

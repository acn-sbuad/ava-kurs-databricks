# Learning Databricks and GitHub Actions
Welcome to this workshop for learning Databricks and GitHub Actions!

For this workshop, your task is to complete a Databricks workbook for cleaning and analyzing data from AirBnB. In addition, you will be setting up workflows for testing and labeling pull requests for the source code.  

## Pre requisites

1. A personal GitHub account. 
If you don't have one, [a GitHub account can be created for free](https://github.com/signup?ref_cta=Sign+up&ref_loc=header+logged+out&ref_page=%2F&source=header-home).

2. A code editor. We recommend [Visual Studio Code](https://code.visualstudio.com/).

3. An [Azure student account](https://azure.microsoft.com/nb-no/free/students/)


## Task 1 - Fork the repository to your own GitHub account

We will be working with a repository that contains two workbooks and unit tests both for the notebooks and dataset. 

Fork [the repository](https://github.com/acn-sbuad/ava-kurs-databricks) to your personal GitHub account, and let's get started! 

## Task 2 - Set up Databricks workspace
 
1. Log onto the [Azure Portal](https://portal.azure.com/)

2. In the search field, type _Databricks_

3. Select `Azure Databricks` under _Services_

    !["Illustration of a successful search in the portal"](imgs/databricks-in-portal.png)

4. You are now redirected to the Databricks service. Click `Create`
    !["Create new"](imgs/create-new.png)

5. Configure the service
    - _Subscription_: leave default opiton
    - _Resource group_: Create a new one, and name it `ava-kurs-rg`
    - _Workspace name_: `ava-kurs-ws`
    - _Region_: `North Europe`
    - _Pricing Tier_: `Standard`
    - [Insert instructions on the memory and GB here]


 6. Click _Review and Create_

    !["Review and create button"](imgs/review-and-create.png)

7. If validation is successful, click `Create`.

This takes a couple of minutes. 
Let's move on to the next task, and get back to this once it's ready.

## Task 3 - Generate a personal access token for GitHub

1. Navigate to your __Git account settings__, then __Developer Settings__. Click the __Personal access tokens__ menu, then click __Generate new token__.

    !["Navigation to token generation"](imgs/generate-new-token.jpg)

2. Select __repo__ as the scope. The token will be applicable for all the specified actions in your repositories.

    !["Configure token"](imgs/configure-token.jpg)

3. Click Generate Token.

    !["Successfully created token page"](imgs/ready-token.jpg)

4. Copy the token value to _Notepad_ or a similar text editor for safe keeping. We will be using it in a later step.

## Task 4 - Connect Databricks to GitHub repository

1. Navigate back to the [Databricks page in the Azure Portal](https://portal.azure.com/#blade/HubsExtension/BrowseResource/resourceType/Microsoft.Databricks%2Fworkspaces)
 
2. Select the workspace you just created from the list
 
3. If the workspace is fully provisioned, you should see the page below. Click `Launch workspace`
    !["Launch workspace page"](imgs/launch-workspace.png)


## Task 5 - Upload testdata

## Task 6 - Set up workflow for running unit tests
Should give student an overview of what is failing and status quoe. 
Helpertests running ok, all tests related to dataset cleaning should be failing


## Task 7 - Clean dataset 

## Task 8 - Set up workflow for labeling PRs based on part of code

## Task 9 - Linear regression on data

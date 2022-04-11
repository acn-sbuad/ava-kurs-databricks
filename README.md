# Learning Databricks and GitHub Actions
Welcome to this workshop for learning Databricks and GitHub Actions!

For this workshop, your task is to complete a Databricks workbook for cleaning and analyzing data from AirBnB. In addition, you will be setting up workflows for testing and labeling pull requests for the source code.  

## Pre requisites

1. A personal GitHub account. 
If you don't have one, [a GitHub account can be created for free](https://github.com/signup?ref_cta=Sign+up&ref_loc=header+logged+out&ref_page=%2F&source=header-home).

2. A code editor. We recommend [Visual Studio Code](https://code.visualstudio.com/).

3. An [Azure student account](https://azure.microsoft.com/nb-no/free/students/)


## Task 1 - Fork the repository to your own GitHub account

We will be working with a repository that contains two workbooks for working on the data set alon with some unit tests.

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
    - _Resource group_: ava-kurs-rg
    - _Workspace name_: ava-kurs-ws
    - _Region_: North Europe
    - _Pricing Tier_: Standard

 6. Click _Review and Create_
    !["Review and create button"](imgs/review-and-create.png)

7. If validation is successful, click `Create`.

## Task 3 - Connect Databricks to repository

## Task 4 - Upload testdata

## Task 5 - Set up workflow for running unit tests
Should give student an overview of what is failing and status quoe. 
Helpertests running ok, all tests related to dataset cleaning should be failing


## Task 6 - Clean dataset 

## Task 7 - Set up workflow for labeling PRs based on part of code

## Task 8 - Linear regression on data

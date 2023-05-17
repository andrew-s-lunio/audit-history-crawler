# Audit History Parser
A lightweight and fast<sup>kind of</sup> method of pulling and reading Lunio account audit history.

**Table of Contents**


### Requirements
1. Python (the script was written with version *3.9.16* but should be able to be run on higher versions).
2. Production AWS access with permissions to read from the `poc-audit-history` bucket (read only minimum).

### Usage
First we need to install the required packages (this only needs to be done once). This can be done with the command:
```shell
pip install -r requirements.txt
```

Once that installation completes, we need to navigate to the [lunio aws landing page](https://lunio.awsapps.com/start#/ "lunio aws landing page") and get our session credentials  by clicking on _Command line or programmatic access_ and following the instructions on screen. These credential normally only last for a short amount of time so this set may need to be repeated.

Once the setup is complete, we should be able to run the file with the command:
```shell
python main.py --account_id=<account_id> 
```
If you are looking for a specific customer or campaign, include the paramater `--aw_id=<customer/campaign_id>` and that will give you the history specific to that customer or campaign. For example:

```shell
python main.py --account_id=7430 --aw_id=1853585311857
```
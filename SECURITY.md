# Security Policy

## Supported Versions

We release patches to fix security vulnerabilities. Which versions are eligible for receiving such patches depends on the CVSS v3.0 Rating:

| Version | Supported          |
| ------- | ------------------ |
| 1.x.x   | :white_check_mark: |
| < 1.0   | :x:                |

## Reporting a Vulnerability

We take the security of Starflow seriously. If you believe you have found a security vulnerability, please report it to us as described below.

**Please do not report security vulnerabilities through public GitHub issues.**

Instead, please report them via email to:

**security@dynoinc.com**

You should receive a response within 48 hours. If for some reason you do not, please follow up via email to ensure we received your original message.

Please include the requested information listed below (as much as you can provide) to help us better understand the nature and scope of the possible issue:

- Type of issue (buffer overflow, SQL injection, cross-site scripting, etc.)
- Full paths of source file(s) related to the vulnerability
- The location of the affected source code (tag/branch/commit or direct URL)
- Any special configuration required to reproduce the issue
- Step-by-step instructions to reproduce the issue
- Proof-of-concept or exploit code (if possible)
- Impact of the issue, including how an attacker might exploit it

This information will help us triage your report more quickly.

## Preferred Languages

We prefer all communications to be in English.

## Disclosure Policy

When we receive a security bug report, we will assign it to a primary handler. This person will coordinate the fix and release process, involving the following steps:

1. Confirm the problem and determine the affected versions.
2. Audit code to find any similar problems.
3. Prepare fixes for all supported versions. These fixes will be released as fast as possible to the main branch.

## Comments on this Policy

If you have suggestions on how this process could be improved, please submit a pull request.

## Security Best Practices

When using Starflow in production:

1. **Keep dependencies updated**: Regularly update your Go dependencies to include security patches
2. **Use HTTPS**: Always use HTTPS in production environments
3. **Validate inputs**: Ensure all inputs to your workflows are properly validated
4. **Monitor logs**: Set up proper logging and monitoring for your workflow executions
5. **Access control**: Implement proper access controls for your workflow store backends
6. **Network security**: Use VPCs and security groups to restrict network access to your infrastructure

## Security Updates

Security updates will be released as patch versions (e.g., 1.0.1, 1.0.2) and will be clearly marked in the release notes. We recommend subscribing to our releases to be notified of security updates. 
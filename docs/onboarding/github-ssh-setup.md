# GitHub SSH Setup on Foot Locker Corporate Network

> Use this guide when HTTPS Git pushes fail due to corporate SSL inspection.
> **Symptom:** `fatal: unable to access '...': Empty reply from server`

---

## Prerequisites

- Git installed on your machine
- A GitHub account (`c013268`)
- Access to [github.com/settings/ssh](https://github.com/settings/ssh/new)

---

## Step 1 — Generate SSH Key

```powershell
ssh-keygen -t ed25519 -C "c013268@footlocker.com"
```

- Press **Enter** to accept the default path (`C:\Users\c013268\.ssh\id_ed25519`)
- Press **Enter** twice to skip passphrase (or set one if you prefer)

---

## Step 2 — Copy Your Public Key

```powershell
Get-Content "$env:USERPROFILE\.ssh\id_ed25519.pub"
```

Copy the **entire output** from the terminal.

---

## Step 3 — Add Key to GitHub

1. Go to → **https://github.com/settings/ssh/new**
2. **Title:** `Foot Locker Laptop`
3. **Key type:** `Authentication Key`
4. **Key:** Paste the copied output
5. Click **Add SSH key**

---

## Step 4 — Test SSH Connection to GitHub

```powershell
ssh -T git@github.com
```

✅ Expected response:
```
Hi c013268! You've successfully authenticated, but GitHub does not provide shell access.
```

> **If this times out**, SSH port 22 may also be blocked. Run the command below to force SSH over port 443, then retry:
> ```powershell
> Add-Content "$env:USERPROFILE\.ssh\config" "Host github.com`n  Hostname ssh.github.com`n  Port 443`n  User git"
> ```

---

## Step 5 — Switch Remote from HTTPS to SSH

```powershell
git remote set-url origin git@github.com:c013268/DOM.git
```

Verify the change:
```powershell
git remote -v
```

You should see:
```
origin  git@github.com:c013268/DOM.git (fetch)
origin  git@github.com:c013268/DOM.git (push)
```

---

## Step 6 — Push

```powershell
git push -u origin main
```

---

## Why This Happens

Foot Locker's corporate network uses **SSL inspection** — a proxy that intercepts HTTPS traffic and re-signs it with a corporate certificate. Git's `schannel` backend tries to verify the certificate revocation list (CRL) but cannot reach the CRL server due to the same firewall, causing it to fail with:

```
schannel: next InitializeSecurityContext failed: CRYPT_E_NO_REVOCATION_CHECK (0x80092012)
```

SSH bypasses HTTPS entirely, which is why it works reliably on corporate networks.

---

## Quick Fix (Alternative — HTTPS only)

If you ever need a quick HTTPS fix without switching to SSH:

```powershell
git config --global http.schannelCheckRevoke false
```

> ⚠️ This disables certificate revocation checking globally. SSH is the preferred long-term solution.

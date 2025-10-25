# HCR Providers Admin — Governance Rules
Rules-Version: v1  
Rules-SHA256: 9fbad15a7cb77dc6ebc09ce7f2e7ac619150d476865dad7bd0f94788eb3b2898  

These rules define the required workflow for modifying `app_admin.py` in the  
`mrbookend/providers-new` repository.

---

## ✅ Rules — AI must obey:

1. Confirm file sync before any change: SHA256 + MD5 + line count.
2. All code changes must be made in GitHub (terminal only for diagnostics).
3. One patch per commit; verify checksums after each patch.
4. Each patch must:
   - Provide existing first + last lines with official line numbers
   - Provide full replacement block beginning at column 0
   - Include a comment anchor at top of replacement block
5. Always bump APP_VER on behavior change.
6. Preserve indentation, imports, all runtime logic.
7. No destructive schema changes or feature removal without explicit user approval.
8. Maintain:
   - Add/Edit/Delete integrity
   - CSV restore (append-only)
   - Search behavior
   - SQLite + Turso (embedded replica) support
9. Assistant must pause and wait for explicit approval before applying changes.
10. Do not show images/graphics unless requested.

---

## ✅ Mandatory Startup Sequence
• User provides terminal SHA256 + MD5 + line count  
• Assistant confirms match with GitHub main HEAD  
• Assistant states: “Ready for Patch 1”

---

This governance document is part of the version-controlled workflow.
Any drift in these rules requires a formal version update and new SHA256.

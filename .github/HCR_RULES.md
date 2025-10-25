# HCR Governance — Enforcement Reference
Rules-Version: v1  
Rules-SHA256: 9fbad15a7cb77dc6ebc09ce7f2e7ac619150d476865dad7bd0f94788eb3b2898  

This ruleset is authoritative for all modifications to `app_admin.py`.

The assistant must reject patches unless:
✔ File sync confirmed  
✔ Patch approved explicitly  
✔ Rules referenced in conversation  

CI checks and maintenance scripts may refer to this file in future updates.

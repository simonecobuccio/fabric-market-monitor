# Adoption notes

Friction log for building fabric-market-monitor on Microsoft Fabric.
Format: date · what I tried · what blocked me · how I solved it

- 2026-11-04 · Create lakehouse with schemas · Unsure where the "Lakehouse schemas" option was · It's a checkbox in the New lakehouse dialog (on by default); schemas are added via Tables → … → New schema
- 2026-11-04 · Connect workspace to GitHub · No "Git integration" in Workspace settings · My workspace doesn't support Git integration; a regular workspace is needed
- 2026-11-04 · Create a new workspace on the Polimi account · "App workspace creation is disabled" by the tenant admin · No fix on that tenant; set up my own Azure/Entra tenant instead
- 2026-11-04 · Use Fabric with personal Outlook account · Fabric doesn't accept personal Microsoft accounts · Created a work user (simone@…onmicrosoft.com) in my own Entra tenant, made it Global Admin
- 2026-11-04 · Start Fabric trial in the new tenant · Power BI trial activated, but "A Fabric trial isn't available for your account" · Open: likely new-tenant provisioning delay; retrying tomorrow, fallback is an F2 capacity with Azure credit
- 2026-11-04 · Prepare Git connection · — · Enabled tenant settings (Fabric items, workspaces, Git/GitHub sync) and created a fine-grained PAT (Contents: read & write, this repo only)

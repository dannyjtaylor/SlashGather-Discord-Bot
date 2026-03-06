# Discord Developer Terms & Policy — Compliance Checklist

This checklist helps ensure **SlashGather** aligns with [Discord's Developer Terms of Service](https://support-dev.discord.com/hc/en-us/articles/8562894815383-Discord-Developer-Terms-of-Service) and [Discord's Developer Policy](https://support-dev.discord.com/hc/en-us/articles/8563934450327-Discord-Developer-Policy). Use it when updating the bot or before App Review.

---

## ✅ Already in place

| Requirement | How SlashGather complies |
|-------------|---------------------------|
| **Privacy policy** | [privacypolicy.md](privacypolicy.md) describes what data is collected, how it’s used, shared, and how users can request deletion. References Discord’s terms. |
| **Terms of Service** | [tos.md](tos.md) requires users to follow Discord ToS/Guidelines, includes age 13+, reporting, and states Discord’s terms control in case of conflict. |
| **No credential collection** | Bot does not ask for or store Discord passwords, login tokens, or account credentials. |
| **No selling/sharing API Data** | Privacy policy states we do not sell, license, or share data with ad networks or data brokers. |
| **Data only for stated functionality** | Data is used for in-game progress, economy, achievements, and notifications only. |
| **User data deletion** | Privacy policy and contact section explain how users can request modification/deletion of their data. |
| **No message content for ML/AI** | Bot does not use Discord message content to train machine learning or AI models. |
| **No scraping/mining** | Bot does not scrape or mine Discord data beyond what’s needed for slash commands and in-app features. |
| **Credentials security** | Discord token is loaded from environment variables (e.g. `DISCORD_TOKEN`), not hardcoded in open source. |
| **DMs** | DMs are only for in-app notifications (e.g. achievements when ephemeral isn’t possible). Privacy policy discloses this and offers opt-out via data deletion/contact. |
| **Reporting path** | ToS and Privacy Policy tell users how to report issues to the Bot operator and to Discord. |

---

## 🔧 Recommended actions (Developer Portal / bot)

1. **Developer Portal**
   - In the [Discord Developer Portal](https://discord.com/developers/applications), add a **public link to your Privacy Policy** (e.g. GitHub raw or your site) in the application’s profile/description so it’s “easily accessible” and in one place Discord can see.

2. **Optional: DM opt-in**
   - Developer Policy says not to contact users without permission. Achievement DMs are tied to app functionality; the privacy policy already describes them and offers opt-out. For extra alignment, you could add an in-bot setting (e.g. “Allow achievement DMs”) so users explicitly opt in.

3. **Monetization**
   - If you later add **paid** features (real money), you must follow [Discord’s Monetization Terms](https://support.discord.com/hc/en-us/articles/5330075836311-Monetization-Terms) and, where applicable, support Discord’s Premium Apps with pricing no higher than elsewhere.

4. **App Review**
   - If Discord requires App Review (e.g. for certain APIs or scale), keep your application description and data practices in the Developer Portal accurate and consistent with [privacypolicy.md](privacypolicy.md) and [tos.md](tos.md).

---

## 📌 Policy links (for Discord / users)

- **Privacy Policy:** [privacypolicy.md](privacypolicy.md) (use your repo URL or hosted link when sharing).
- **Terms of Service:** [tos.md](tos.md) (use your repo URL or hosted link when sharing).
- **Discord Developer ToS:** https://support-dev.discord.com/hc/en-us/articles/8562894815383-Discord-Developer-Terms-of-Service  
- **Discord Developer Policy:** https://support-dev.discord.com/hc/en-us/articles/8563934450327-Discord-Developer-Policy  

---

*This checklist is for internal use and does not constitute legal advice. When in doubt, refer to Discord’s official terms and policies.*

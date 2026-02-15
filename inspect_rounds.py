# inspect_rounds.py
from playwright.sync_api import sync_playwright

def wait(page, ms=3000):
    page.wait_for_timeout(ms)

def dismiss_modals(page):
    selectors = [
        'button:has-text("Accept")',
        'button:has-text("Agree")',
        'button:has-text("No thanks")',
        '[aria-label="Close"]',
    ]
    for selector in selectors:
        try:
            btn = page.locator(selector).first
            if btn.is_visible(timeout=500):
                btn.click()
                wait(page, 500)
        except:
            pass

def save_page(page, filename):
    html = page.content()
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"✅ Saved {filename}")

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720}
        )
        page = context.new_page()

        print("Loading matches page...")
        page.goto(
            "https://r6.tracker.network/r6siege/profile/ubi/SaucedZyn/matches",
            wait_until='domcontentloaded'
        )
        wait(page, 4000)
        dismiss_modals(page)

        # Click first match row
        print("Opening first match...")
        try:
            match = page.locator('.v3-match-row').first
            match.click()
            wait(page, 3000)
            dismiss_modals(page)
            save_page(page, 'dump_match_expanded.html')
            print("✅ Match expanded")
        except Exception as e:
            print(f"⚠️ Match click: {e}")
            input("Press Enter to close...")
            return

        # Now look for round entries and click one
        print("Looking for round data...")
        try:
            # From your codegen: rounds appear as expandable items
            rounds = page.locator('[class*="round"]').all()
            print(f"Found {len(rounds)} round elements")
            
            # Try clicking "Rounds" tab or button if present
            try:
                page.get_by_text("Rounds", exact=True).click()
                wait(page, 2000)
                save_page(page, 'dump_rounds_tab.html')
                print("✅ Rounds tab saved")
            except:
                pass

            # Try clicking first round entry
            for i in range(min(3, len(rounds))):
                try:
                    r = rounds[i]
                    text = r.inner_text()
                    if 'Rnd' in text or 'Round' in text:
                        print(f"Clicking round: {text[:50]}")
                        r.click()
                        wait(page, 2000)
                        save_page(page, f'dump_round_{i}.html')
                        break
                except:
                    pass
        except Exception as e:
            print(f"⚠️ Round click: {e}")

        input("\nAll done. Press Enter to close...")
        context.close()
        browser.close()

run()
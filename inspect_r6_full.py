# inspect_r6_full.py
from playwright.sync_api import sync_playwright

def save_page(page, filename):
    html = page.content()
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html)
    print(f"✅ Saved {filename}")

def wait(page, ms=3000):
    page.wait_for_timeout(ms)

def dismiss_modals(page):
    """Close any popups blocking the page."""
    selectors = [
        'button:has-text("Accept")',
        'button:has-text("Agree")', 
        'button:has-text("Got it")',
        'button:has-text("No thanks")',
        'button:has-text("Maybe later")',
        '[aria-label="Close"]',
        '[aria-label="Dismiss"]',
    ]
    for selector in selectors:
        try:
            btn = page.locator(selector).first
            if btn.is_visible(timeout=500):
                btn.click()
                wait(page, 500)
                print(f"  → Dismissed: {selector}")
        except:
            pass

def close_season_drawer(page):
    """Close the expanded season stats drawer."""
    try:
        # From your codegen - this was the close click
        page.locator(".size-6.cursor-pointer").first.click()
        wait(page, 1000)
        print("  → Closed season drawer")
    except Exception as e:
        print(f"  → Could not close drawer: {e}")

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        
        # Use a realistic user agent to avoid Cloudflare
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720}
        )
        page = context.new_page()

        # 1. Load profile
        print("\n--- LOADING PROFILE ---")
        page.goto(
            "https://r6.tracker.network/r6siege/profile/ubi/SaucedZyn/overview",
            wait_until='domcontentloaded'
        )
        wait(page, 4000)
        dismiss_modals(page)
        save_page(page, 'dump_overview.html')
        print("✅ Overview saved")

        # 2. Expand season drawer
        print("\n--- SEASON DRAWER ---")
        try:
            page.get_by_role("button", name="View All Stats").click()
            wait(page, 2000)
            save_page(page, 'dump_season_expanded.html')
            print("✅ Season expanded saved")
            
            # Close drawer before navigating
            close_season_drawer(page)
            wait(page, 1000)
        except Exception as e:
            print(f"⚠️  Season: {e}")

        # 3. Maps
        print("\n--- MAPS ---")
        dismiss_modals(page)
        try:
            page.get_by_role("link", name="Maps").click()
            wait(page, 3000)
            dismiss_modals(page)
            
            # Filter ranked
            try:
                page.get_by_role("button", name="Ranked", exact=True).click()
                wait(page, 2000)
            except:
                print("  → No ranked filter found")
            
            save_page(page, 'dump_maps.html')
            print("✅ Maps saved")
        except Exception as e:
            print(f"⚠️  Maps: {e}")

        # 4. Operators
        print("\n--- OPERATORS ---")
        dismiss_modals(page)
        try:
            page.get_by_role("link", name="Operators").click()
            wait(page, 3000)
            dismiss_modals(page)
            save_page(page, 'dump_operators.html')
            print("✅ Operators saved")
        except Exception as e:
            print(f"⚠️  Operators: {e}")

        # 5. Matches
        print("\n--- MATCHES ---")
        dismiss_modals(page)
        try:
            page.get_by_role("link", name="Matches", exact=True).first.click()
            wait(page, 3000)
            dismiss_modals(page)
            save_page(page, 'dump_matches.html')
            print("✅ Matches saved")
        except Exception as e:
            print(f"⚠️  Matches: {e}")

        # 6. First match detail
        print("\n--- MATCH DETAIL ---")
        dismiss_modals(page)
        try:
            page.locator('.v3-match-row').first.click()
            wait(page, 3000)
            dismiss_modals(page)
            save_page(page, 'dump_match_detail.html')
            print("✅ Match detail saved")
        except Exception as e:
            print(f"⚠️  Match detail: {e}")

        print("\n✅ Done.")
        input("Press Enter to close...")
        context.close()
        browser.close()

run()
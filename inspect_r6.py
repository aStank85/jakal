# inspect_r6.py
from playwright.sync_api import sync_playwright
import json

def inspect_page(url: str, username: str):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        print(f"Loading {username} profile...")
        page.goto(url)
        page.wait_for_load_state('networkidle')
        
        # Click View All Stats if present
        try:
            page.get_by_role("button", name="View All Stats").click()
            page.wait_for_load_state('networkidle')
            print("✅ Expanded full stats")
        except:
            print("⚠️  No 'View All Stats' button found")
        
        # Save full page HTML for inspection
        html = page.content()
        with open('page_dump.html', 'w', encoding='utf-8') as f:
            f.write(html)
        print("✅ Saved page_dump.html")
        
        # Try to find stat containers
        print("\n--- LOOKING FOR STAT ELEMENTS ---")
        
        # Common R6 Tracker selectors
        selectors_to_try = [
            '[class*="stat"]',
            '[class*="card"]', 
            '[class*="segment"]',
            '[class*="value"]',
            '[class*="label"]',
            '[data-stat]',
        ]
        
        for selector in selectors_to_try:
            elements = page.query_selector_all(selector)
            if elements:
                print(f"\n{selector}: {len(elements)} elements found")
                # Show first 3
                for el in elements[:3]:
                    text = el.inner_text().strip()[:100]
                    if text:
                        print(f"  → '{text}'")
        
        input("\nPress Enter to close browser...")
        browser.close()

inspect_page(
    "https://r6.tracker.network/r6siege/profile/ubi/SaucedZyn/overview",
    "SaucedZyn"
)
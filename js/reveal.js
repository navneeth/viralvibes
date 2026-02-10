document.addEventListener("DOMContentLoaded", () => {
  const items = document.querySelectorAll("[data-reveal]");

  // Safety fallback: reveal everything if JS runs
  if (!items.length) return;

  const observer = new IntersectionObserver(
    entries => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          entry.target.classList.add("revealed");
          observer.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.15 }
  );

  items.forEach(el => observer.observe(el));
});

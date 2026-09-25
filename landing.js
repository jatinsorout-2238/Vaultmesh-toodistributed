/**
 * VaultMesh — Public Landing Page Interactivity
 * Copy-to-clipboard, smooth scroll anchors, and interactive visual feedback
 */

document.addEventListener('DOMContentLoaded', () => {
  // 1. Copy CLI Install Command
  const copyBtn = document.getElementById('btn-copy-install');
  const cmdText = document.getElementById('install-cmd');
  const label = document.getElementById('copy-btn-label');

  if (copyBtn && cmdText && label) {
    copyBtn.addEventListener('click', async () => {
      try {
        await navigator.clipboard.writeText(cmdText.textContent.trim());
        label.textContent = 'Copied!';
        copyBtn.style.borderColor = 'var(--brand-emerald)';
        copyBtn.style.color = 'var(--brand-emerald)';

        setTimeout(() => {
          label.textContent = 'Copy';
          copyBtn.style.borderColor = '';
          copyBtn.style.color = '';
        }, 2000);
      } catch (err) {
        // Fallback for older browsers
        const textarea = document.createElement('textarea');
        textarea.value = cmdText.textContent.trim();
        document.body.appendChild(textarea);
        textarea.select();
        document.execCommand('copy');
        document.body.removeChild(textarea);

        label.textContent = 'Copied!';
        setTimeout(() => {
          label.textContent = 'Copy';
        }, 2000);
      }
    });
  }

  // 2. Smooth scrolling for internal anchor links
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
      const targetId = this.getAttribute('href').substring(1);
      if (!targetId) return;

      const targetElem = document.getElementById(targetId);
      if (targetElem) {
        e.preventDefault();
        targetElem.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    });
  });

  // 3. Header scroll shadow enhancement
  const header = document.querySelector('.site-header');
  window.addEventListener('scroll', () => {
    if (window.scrollY > 20) {
      header.style.boxShadow = '0 4px 20px rgba(0, 0, 0, 0.05)';
      header.style.background = 'rgba(255, 255, 255, 0.96)';
    } else {
      header.style.boxShadow = 'none';
      header.style.background = 'rgba(255, 255, 255, 0.92)';
    }
  });

  // 4. Scroll-Triggered Fade-In Reveal Animations for Sections & Cards
  if ('IntersectionObserver' in window) {
    const observer = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          entry.target.classList.add('is-visible');
          observer.unobserve(entry.target);
        }
      });
    }, {
      rootMargin: '0px 0px -40px 0px',
      threshold: 0.1
    });

    document.querySelectorAll('.section-anchor-block, .feature-info-card, .news-article-card, .section-study-contribute, .section-deploy-manage').forEach((el, idx) => {
      el.classList.add('reveal-on-scroll');
      if (el.classList.contains('feature-info-card') || el.classList.contains('news-article-card')) {
        el.style.transitionDelay = `${(idx % 3) * 100}ms`;
      }
      observer.observe(el);
    });
  }
});

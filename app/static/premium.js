/**
 * AZ Stream — Premium JavaScript Layer
 * =====================================
 * Features:
 *  - Lenis smooth scrolling
 *  - GSAP + ScrollTrigger global setup
 *  - Scroll progress bar
 *  - Page transition system
 *  - Toast notification system
 *  - Mobile navigation drawer
 *  - Animated counter (count-up)
 *  - Skeleton loading helpers
 *  - Lazy loading with IntersectionObserver
 *  - Micro-interaction helpers
 */

(function () {
  'use strict';

  /* ============================================================
     1. UTILITIES
  ============================================================ */
  const prefersReducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  function $(sel, ctx) { return (ctx || document).querySelector(sel); }
  function $$(sel, ctx) { return Array.from((ctx || document).querySelectorAll(sel)); }

  /* ============================================================
     2. SCROLL PROGRESS BAR
  ============================================================ */
  function initScrollProgress() {
    const bar = document.createElement('div');
    bar.id = 'scroll-progress';
    document.body.prepend(bar);

    function updateProgress() {
      const scrollTop = window.scrollY;
      const docHeight = document.documentElement.scrollHeight - window.innerHeight;
      const pct = docHeight > 0 ? (scrollTop / docHeight) * 100 : 0;
      bar.style.width = pct + '%';
    }

    window.addEventListener('scroll', updateProgress, { passive: true });
    updateProgress();
  }

  /* ============================================================
     3. PAGE TRANSITION SYSTEM
  ============================================================ */
  function initPageTransitions() {
    if (prefersReducedMotion) return;

    const overlay = document.createElement('div');
    overlay.id = 'page-transition-overlay';
    overlay.innerHTML = '<div class="page-transition-logo">AZ Stream</div>';
    document.body.prepend(overlay);

    // Animate in on load
    requestAnimationFrame(() => {
      overlay.classList.add('leaving');
      setTimeout(() => {
        overlay.style.display = 'none';
        overlay.classList.remove('leaving');
      }, 500);
    });

    // Intercept internal links
    document.addEventListener('click', function (e) {
      const link = e.target.closest('a');
      if (!link) return;
      const href = link.getAttribute('href');
      if (!href || href.startsWith('#') || href.startsWith('http') || href.startsWith('https') ||
          href.startsWith('tg') || href.startsWith('mailto') || link.target === '_blank' ||
          link.hasAttribute('data-no-transition') || link.hasAttribute('download')) return;

      e.preventDefault();
      overlay.style.display = 'flex';
      requestAnimationFrame(() => {
        overlay.classList.add('entering');
        setTimeout(() => {
          window.location.href = href;
        }, 380);
      });
    });
  }

  /* ============================================================
     4. TOAST NOTIFICATION SYSTEM
  ============================================================ */
  let toastContainer;

  function initToasts() {
    toastContainer = document.createElement('div');
    toastContainer.id = 'toast-container';
    document.body.appendChild(toastContainer);
  }

  const TOAST_ICONS = {
    success: '✓',
    error: '✕',
    info: 'ℹ',
    warning: '⚠',
  };

  /**
   * Show a toast notification
   * @param {string} message
   * @param {string} type - 'success' | 'error' | 'info' | 'warning'
   * @param {string} [title]
   * @param {number} [duration=4000]
   */
  window.AZToast = function (message, type = 'info', title = '', duration = 4000) {
    if (!toastContainer) initToasts();

    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.innerHTML = `
      <div class="toast-icon">${TOAST_ICONS[type] || 'ℹ'}</div>
      <div class="toast-content">
        ${title ? `<div class="toast-title">${title}</div>` : ''}
        <div class="toast-message">${message}</div>
      </div>
      <div class="toast-bar"></div>
    `;

    toastContainer.appendChild(toast);
    requestAnimationFrame(() => {
      requestAnimationFrame(() => {
        toast.classList.add('toast-visible');
      });
    });

    const dismiss = () => {
      toast.classList.add('toast-hiding');
      toast.classList.remove('toast-visible');
      setTimeout(() => toast.remove(), 350);
    };

    toast.addEventListener('click', dismiss);
    const timer = setTimeout(dismiss, duration);
    toast.addEventListener('mouseenter', () => clearTimeout(timer));
  };

  /* ============================================================
     5. MOBILE NAVIGATION DRAWER
  ============================================================ */
  function initMobileNav() {
    const header = $('.site-header');
    const headerRail = $('.header-rail');
    if (!header || !headerRail) return;

    // Create toggle button
    const toggle = document.createElement('button');
    toggle.id = 'mobile-nav-toggle';
    toggle.setAttribute('aria-label', 'Open menu');
    toggle.innerHTML = `
      <span class="nav-hamburger-line"></span>
      <span class="nav-hamburger-line"></span>
      <span class="nav-hamburger-line"></span>
    `;
    headerRail.prepend(toggle);

    // Create overlay
    const overlay = document.createElement('div');
    overlay.id = 'mobile-nav-overlay';
    document.body.appendChild(overlay);

    // Create drawer with nav links
    const navLinks = $$('.site-nav a');
    const drawer = document.createElement('nav');
    drawer.id = 'mobile-nav-drawer';
    drawer.setAttribute('aria-label', 'Mobile navigation');

    const closeBtn = document.createElement('button');
    closeBtn.className = 'mobile-nav-close';
    closeBtn.innerHTML = '✕';
    closeBtn.setAttribute('aria-label', 'Close menu');
    drawer.appendChild(closeBtn);

    const linkDefs = [
      { href: '/', icon: '🏠', label: 'Home' },
      { href: '/sections', icon: '📂', label: 'Public Sections' },
      { href: '/trending', icon: '🔥', label: 'Trending' },
      { href: '/bots', icon: '🤖', label: 'Bot Network' },
    ];

    const currentPath = window.location.pathname;
    linkDefs.forEach(({ href, icon, label }) => {
      const a = document.createElement('a');
      a.href = href;
      a.className = 'mobile-nav-link' + (currentPath === href ? ' active' : '');
      a.innerHTML = `<span class="mobile-nav-icon">${icon}</span> ${label}`;
      a.setAttribute('data-no-transition', '');
      drawer.appendChild(a);
    });

    // Re-add data-no-transition to suppress page transition from mobile nav
    drawer.querySelectorAll('a').forEach(a => {
      a.addEventListener('click', () => closeDrawer());
    });

    document.body.appendChild(drawer);

    function openDrawer() {
      drawer.classList.add('open');
      overlay.classList.add('open');
      toggle.classList.add('open');
      toggle.setAttribute('aria-label', 'Close menu');
      document.body.style.overflow = 'hidden';
    }

    function closeDrawer() {
      drawer.classList.remove('open');
      overlay.classList.remove('open');
      toggle.classList.remove('open');
      toggle.setAttribute('aria-label', 'Open menu');
      document.body.style.overflow = '';
    }

    toggle.addEventListener('click', () => {
      drawer.classList.contains('open') ? closeDrawer() : openDrawer();
    });
    closeBtn.addEventListener('click', closeDrawer);
    overlay.addEventListener('click', closeDrawer);

    document.addEventListener('keydown', e => {
      if (e.key === 'Escape') closeDrawer();
    });
  }

  /* ============================================================
     6. LENIS SMOOTH SCROLLING
  ============================================================ */
  function initLenis() {
    if (prefersReducedMotion) return;
    if (typeof Lenis === 'undefined') return;

    const lenis = new Lenis({
      duration: 1.15,
      easing: (t) => Math.min(1, 1.001 - Math.pow(2, -10 * t)),
      orientation: 'vertical',
      gestureOrientation: 'vertical',
      smoothWheel: true,
      wheelMultiplier: 1,
      touchMultiplier: 2,
    });

    // Connect to GSAP ScrollTrigger if available
    if (typeof gsap !== 'undefined' && typeof ScrollTrigger !== 'undefined') {
      lenis.on('scroll', ScrollTrigger.update);
      gsap.ticker.add((time) => lenis.raf(time * 1000));
      gsap.ticker.lagSmoothing(0);
    } else {
      function raf(time) {
        lenis.raf(time);
        requestAnimationFrame(raf);
      }
      requestAnimationFrame(raf);
    }

    window._lenis = lenis;
  }

  /* ============================================================
     7. GSAP SCROLL ANIMATIONS
  ============================================================ */
  function initGSAPAnimations() {
    if (prefersReducedMotion) return;
    if (typeof gsap === 'undefined' || typeof ScrollTrigger === 'undefined') return;

    gsap.registerPlugin(ScrollTrigger);

    // Animate all .gsap-hidden elements
    $$('.gsap-hidden').forEach(el => {
      gsap.to(el, {
        opacity: 1,
        y: 0,
        duration: 0.7,
        ease: 'power3.out',
        scrollTrigger: {
          trigger: el,
          start: 'top 90%',
          once: true,
        },
      });
    });

    // Stagger children inside .gsap-stagger-parent
    $$('.gsap-stagger-parent').forEach(parent => {
      const children = $$('.gsap-stagger-child', parent);
      gsap.from(children, {
        opacity: 0,
        y: 25,
        duration: 0.55,
        stagger: 0.09,
        ease: 'power2.out',
        scrollTrigger: {
          trigger: parent,
          start: 'top 85%',
          once: true,
        },
      });
    });

    // Horizontal slide-in for .gsap-fade-left
    $$('.gsap-fade-left').forEach(el => {
      gsap.to(el, {
        opacity: 1,
        x: 0,
        duration: 0.7,
        ease: 'power3.out',
        scrollTrigger: {
          trigger: el,
          start: 'top 88%',
          once: true,
        },
      });
    });

    $$('.gsap-fade-right').forEach(el => {
      gsap.to(el, {
        opacity: 1,
        x: 0,
        duration: 0.7,
        ease: 'power3.out',
        scrollTrigger: {
          trigger: el,
          start: 'top 88%',
          once: true,
        },
      });
    });
  }

  /* ============================================================
     8. COUNT-UP ANIMATION
  ============================================================ */
  function animateCountUp(el, target, duration) {
    if (prefersReducedMotion) { el.textContent = target; return; }
    const start = 0;
    const startTime = performance.now();
    const isFloat = target.toString().includes('.');
    const suffix = el.dataset.suffix || '';

    function update(now) {
      const elapsed = now - startTime;
      const progress = Math.min(elapsed / duration, 1);
      // Ease out cubic
      const eased = 1 - Math.pow(1 - progress, 3);
      const current = isFloat
        ? (start + (target - start) * eased).toFixed(1)
        : Math.round(start + (target - start) * eased);
      el.textContent = current.toLocaleString() + suffix;
      if (progress < 1) requestAnimationFrame(update);
    }

    requestAnimationFrame(update);
  }

  function initCountUpAnimations() {
    if (typeof IntersectionObserver === 'undefined') return;
    const observer = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          const el = entry.target;
          const target = parseFloat(el.dataset.target || '0');
          animateCountUp(el, target, 1800);
          observer.unobserve(el);
        }
      });
    }, { threshold: 0.3 });

    $$('.count-up-number').forEach(el => observer.observe(el));
  }

  /* ============================================================
     9. LAZY LOADING WITH INTERSECTION OBSERVER
  ============================================================ */
  function initLazyLoading() {
    if (typeof IntersectionObserver === 'undefined') return;

    const observer = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          const img = entry.target;
          const src = img.dataset.src;
          if (src) {
            img.src = src;
            img.removeAttribute('data-src');
            img.classList.add('lazy-loaded');
          }
          observer.unobserve(img);
        }
      });
    }, { rootMargin: '200px 0px' });

    $$('img[data-src]').forEach(img => observer.observe(img));
  }

  /* ============================================================
     10. LIKE BUTTON MICRO-INTERACTION
  ============================================================ */
  function initLikeAnimations() {
    document.addEventListener('click', function (e) {
      const btn = e.target.closest('#like-btn');
      if (!btn) return;
      btn.classList.remove('like-burst');
      void btn.offsetWidth; // reflow
      btn.classList.add('like-burst');
    });
  }

  /* ============================================================
     11. CARD TILT EFFECT (subtle 3D on hover, desktop only)
  ============================================================ */
  function initCardTilt() {
    if (prefersReducedMotion) return;
    if (window.innerWidth < 1024) return;

    $$('.feature-card, .public-section-card').forEach(card => {
      card.addEventListener('mousemove', function (e) {
        const rect = card.getBoundingClientRect();
        const x = (e.clientX - rect.left) / rect.width - 0.5;
        const y = (e.clientY - rect.top) / rect.height - 0.5;
        card.style.transform = `perspective(800px) rotateY(${x * 8}deg) rotateX(${-y * 8}deg) translateZ(4px)`;
      });
      card.addEventListener('mouseleave', function () {
        card.style.transform = '';
      });
    });
  }

  /* ============================================================
     12. CLIENT-SIDE SECTION SEARCH FILTER
  ============================================================ */
  window.AZSearch = {
    init: function (inputId, itemSelector, noResultsId) {
      const input = document.getElementById(inputId);
      if (!input) return;

      const getItems = () => $$(itemSelector);
      const noResults = noResultsId ? document.getElementById(noResultsId) : null;

      input.addEventListener('input', function () {
        const query = this.value.toLowerCase().trim();
        let visibleCount = 0;

        getItems().forEach(item => {
          const text = item.textContent.toLowerCase();
          const match = !query || text.includes(query);
          item.style.display = match ? '' : 'none';
          if (match) visibleCount++;
        });

        if (noResults) {
          noResults.style.display = visibleCount === 0 ? 'block' : 'none';
        }

        const counter = document.getElementById(inputId + '-count');
        if (counter) {
          counter.textContent = query
            ? `${visibleCount} result${visibleCount !== 1 ? 's' : ''}`
            : '';
        }
      });
    }
  };

  /* ============================================================
     13. HEADER ACTIVE NAV LINK ENHANCEMENT
  ============================================================ */
  function enhanceNavLinks() {
    const path = window.location.pathname;
    $$('.site-nav a, .mobile-nav-link').forEach(link => {
      const href = link.getAttribute('href');
      if (!href) return;
      const isActive = href === path || (href !== '/' && path.startsWith(href));
      link.classList.toggle('active', isActive);
    });
  }

  /* ============================================================
     14. INIT ALL
  ============================================================ */
  function init() {
    initScrollProgress();
    initToasts();
    initMobileNav();
    initLazyLoading();
    initLikeAnimations();
    enhanceNavLinks();

    // Wait for CDN libraries
    const checkLibs = setInterval(() => {
      clearInterval(checkLibs);
      initPageTransitions();
      initLenis();
      initGSAPAnimations();
      initCountUpAnimations();
      initCardTilt();
    }, 100);
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

  /* ============================================================
     15. EXPOSE GLOBAL API
  ============================================================ */
  window.AZStream = {
    toast: window.AZToast,
    search: window.AZSearch,
    refresh: function () {
      if (window._lenis) window._lenis.resize();
      if (typeof ScrollTrigger !== 'undefined') ScrollTrigger.refresh();
    }
  };

})();

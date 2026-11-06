(() => {
    if (window.vvVideoDeepDive) return;
    window.vvVideoDeepDive = true;
    const cache = new Map();

    function probe(url) {
        return new Promise(resolve => {
            const image = new Image();
            let timer;
            const finish = available => {
                clearTimeout(timer);
                image.onload = image.onerror = null;
                resolve(available);
            };
            image.onload = () => finish(image.naturalWidth > 120);
            image.onerror = () => finish(false);
            timer = setTimeout(() => finish(false), 10000);
            image.src = url;
        });
    }

    function resolveFrame(video, frame, retry) {
        const key = `${video}:${frame}`;
        if (retry) cache.delete(key);
        if (!cache.has(key)) {
            if (cache.size >= 128) cache.delete(cache.keys().next().value);
            cache.set(key, (async () => {
                for (const quality of ['maxres', 'hq']) {
                    const url = `https://i.ytimg.com/vi/${video}/${quality}${frame}.jpg`;
                    if (await probe(url)) return url;
                }
                return null;
            })());
        }
        return cache.get(key);
    }

    async function selectFrame(card, frame, retry = false) {
        const request = String(Number(card.dataset.vvRequest || 0) + 1);
        card.dataset.vvRequest = request;
        card.dataset.vvSelectedFrame = String(frame);
        card.querySelectorAll('[data-vv-frame-choice]').forEach(button => {
            button.setAttribute('aria-pressed', String(button.dataset.vvFrameChoice === String(frame)));
        });
        const image = card.querySelector('[data-vv-frame-image]');
        const placeholder = card.querySelector('[data-vv-frame-placeholder]');
        const status = card.querySelector('[data-vv-frame-status]');
        image.classList.add('hidden');
        placeholder.classList.remove('hidden');
        placeholder.textContent = 'Loading frame...';
        status.textContent = 'Frame loading';
        const url = await resolveFrame(card.dataset.vvVideo, frame, retry);
        if (!card.isConnected || card.dataset.vvRequest !== request) return;
        if (url) {
            image.onerror = () => {
                if (!card.isConnected || card.dataset.vvRequest !== request) return;
                cache.delete(`${card.dataset.vvVideo}:${frame}`);
                image.classList.add('hidden');
                placeholder.classList.remove('hidden');
                placeholder.textContent = 'No frame available';
                status.textContent = 'Frame unavailable';
            };
            image.onload = () => {
                if (image.naturalWidth <= 120) image.onerror();
            };
            image.src = url;
            image.classList.remove('hidden');
            placeholder.classList.add('hidden');
            status.textContent = 'Frame available';
        } else {
            placeholder.textContent = 'No frame available';
            status.textContent = 'Frame unavailable';
        }
    }

    function officialLoaded(image) {
        if (!image.matches('[data-vv-official]')) return;
        if (image.naturalWidth > 120) return;
        if (!image.dataset.vvOfficialFallback) {
            image.dataset.vvOfficialFallback = 'true';
            image.src = `https://i.ytimg.com/vi/${image.dataset.vvVideoId}/hqdefault.jpg`;
        } else {
            image.classList.add('hidden');
            image.parentElement.querySelector('[data-vv-official-missing]').classList.remove('hidden');
        }
    }

    document.addEventListener('load', event => {
        if (event.target instanceof HTMLImageElement) officialLoaded(event.target);
    }, true);
    document.addEventListener('error', event => {
        if (event.target instanceof HTMLImageElement) officialLoaded(event.target);
    }, true);
    document.addEventListener('click', event => {
        const button = event.target.closest('[data-vv-frame-choice], [data-vv-frame-retry]');
        if (!button) return;
        const card = button.closest('[data-vv-video]');
        observer?.unobserve(card);
        selectFrame(card, Number(button.dataset.vvFrameChoice || card.dataset.vvSelectedFrame),
            button.hasAttribute('data-vv-frame-retry'));
    });

    const observer = 'IntersectionObserver' in window ? new IntersectionObserver(entries => {
        entries.forEach(entry => {
            if (!entry.isIntersecting) return;
            observer.unobserve(entry.target);
            selectFrame(entry.target, Number(entry.target.dataset.vvSelectedFrame));
        });
    }, {rootMargin: '200px'}) : null;

    function initialize(root) {
        root.querySelectorAll('[data-vv-video]').forEach(card => {
            if (card.dataset.vvInitialized) return;
            card.dataset.vvInitialized = 'true';
            if (observer) observer.observe(card);
            else selectFrame(card, Number(card.dataset.vvSelectedFrame));
        });
        root.querySelectorAll('[data-vv-official]').forEach(image => {
            if (image.complete) officialLoaded(image);
        });
    }
    initialize(document);
    document.addEventListener('htmx:afterSwap', () => initialize(document));
})();
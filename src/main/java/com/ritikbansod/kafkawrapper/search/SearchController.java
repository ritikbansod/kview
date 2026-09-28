package com.ritikbansod.kafkawrapper.search;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Whole-topic background search: start (POST — a read-shaped mutation), poll
 * progress + results, cancel. The search itself is read-only: never commits,
 * never joins a real group.
 */
@RestController
@RequestMapping("/api/clusters/{clusterId}")
public class SearchController {

    private final SearchService searchService;

    public SearchController(SearchService searchService) {
        this.searchService = searchService;
    }

    @PostMapping("/topics/{topic}/search")
    public Map<String, String> start(@PathVariable String clusterId, @PathVariable String topic,
                                     @RequestBody SearchService.SearchRequest request) {
        return Map.of("searchId", searchService.start(clusterId, topic, request));
    }

    @GetMapping("/searches/{searchId}")
    public SearchService.SearchStatus status(@PathVariable String clusterId, @PathVariable String searchId) {
        return searchService.status(clusterId, searchId);
    }

    @DeleteMapping("/searches/{searchId}")
    public Map<String, String> cancel(@PathVariable String clusterId, @PathVariable String searchId) {
        searchService.cancel(clusterId, searchId);
        return Map.of("searchId", searchId, "status", "cancelling");
    }
}

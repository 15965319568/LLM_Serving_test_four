"""Incremental stop matching, including stop strings split across tokens."""


class StopFilter:
    def __init__(self, stops):
        self.stops = tuple(stops)
        self.pending = ''
        self.stopped = False

    def feed(self, text):
        if self.stopped:
            return ''
        self.pending += text
        positions = [self.pending.find(s) for s in self.stops if s in self.pending]
        if positions:
            end = min(positions)
            output, self.pending = self.pending[:end], ''
            self.stopped = True
            return output
        hold = 0
        for stop in self.stops:
            for size in range(1, min(len(stop), len(self.pending) + 1)):
                if self.pending.endswith(stop[:size]):
                    hold = max(hold, size)
        if hold:
            output, self.pending = self.pending[:-hold], self.pending[-hold:]
        else:
            output, self.pending = self.pending, ''
        return output

    def finish(self):
        output, self.pending = self.pending, ''
        return output
